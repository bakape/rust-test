use serde::{Deserialize, Serialize};
use std::{
	collections::HashMap,
	error::Error,
	fs::File,
	io::{stdout, BufReader, BufWriter, Read, Write},
};

// TODO: go through PDF and assert everything is covered

fn main() -> Result<(), String> {
	let path = match std::env::args().nth(1) {
		Some(p) => p,
		None => {
			return Err(
				"file path required as the first and only parameter".into()
			)
		}
	};

	// Buffer to reduce syscalls.
	//
	// Opted not to use multithreading or even a single-threaded event loop, as
	// synchronisation costs can outweigh the benefits of concurrent processing
	// in this single input case.
	// Benchmarks with near-real inputs would be required to ascertain this,
	// but less complexity is a safe default.
	//
	// The process() function can be converted to run asynchronously on a
	// multithreaded Tokio runtime, if this application is to be adapted
	// for concurrent multiple request handling.
	(|| {
		process(
			&mut BufWriter::new(stdout()),
			&mut BufReader::new(File::open(path)?),
		)
	})()
	.map_err(|e| e.to_string())
}

/// Process a CSV stream `r` and write the account status CSV to `w`
fn process(
	w: &mut impl Write,
	r: &mut impl Read,
) -> Result<(), Box<dyn Error>> {
	let mut accounts = HashMap::<u16, Account>::with_capacity(64);

	// Read input CSV rows
	for res in csv::ReaderBuilder::new()
		.trim(csv::Trim::All)
		.from_reader(r)
		.deserialize()
	{
		let row: InRow = res?;
		let acc = accounts.entry(row.client).or_default();

		match (&row.typ, &row.amount) {
			(TxType::Deposit, Some(amount)) => {
				let amount = to_minor(*amount);
				acc.available += amount;
				acc.deposits.insert(
					row.tx,
					Deposit {
						dispute_state: DisputeState::NotInitiated,
						amount,
					},
				);
			}
			(TxType::Withdrawal, Some(amount)) => {
				// The task definition did not specify what exactly locking an
				// account entails.The term "freeze" was also used to describe
				// locking, so I went with the Investopedia  definition of
				// allowing deposits, but not withdrawals.
				// Further disputes and chargebacks are also allowed on locked
				// accounts, based on my understanding of what the business
				// logic should be in those cases.
				if !acc.locked {
					let amount = to_minor(*amount);
					if acc.available >= amount {
						acc.available -= amount;
					}
				}
			}
			(TxType::Dispute, _) => {
				if let Some(d) = acc.deposits.get_mut(&row.tx) {
					if matches!(d.dispute_state, DisputeState::NotInitiated) {
						d.dispute_state = DisputeState::Initiated;
						acc.available -= d.amount;
						acc.held += d.amount;
					}
				}
			}
			(TxType::Resolve, _) => {
				if let Some(d) = acc.deposits.get_mut(&row.tx) {
					if matches!(d.dispute_state, DisputeState::Initiated) {
						// Enable starting another dispute
						d.dispute_state = DisputeState::NotInitiated;

						acc.available += d.amount;
						acc.held -= d.amount;
					}
				}
			}
			(TxType::Chargeback, _) => {
				if let Some(d) = acc.deposits.get_mut(&row.tx) {
					if matches!(d.dispute_state, DisputeState::Initiated) {
						d.dispute_state = DisputeState::ChargedBack;
						acc.held -= d.amount;
						acc.locked = true;
					}
				}
			}
			// Ignoring invalid cases to match behaviour of all other
			// validations
			_ => (),
		}
	}

	// Dump output as CSV
	let mut w = csv::Writer::from_writer(w);
	for (cl, acc) in accounts {
		w.serialize(OutRow {
			client: cl,
			available: to_major(acc.available),
			held: to_major(acc.held),
			total: to_major(acc.available + acc.held),
			locked: acc.locked,
		})?;
	}

	Ok(())
}

/// A row of the input CSV file
#[derive(Deserialize)]
struct InRow {
	/// Transaction type
	#[serde(rename = "type")]
	typ: TxType,

	/// Client ID
	client: u16,

	/// Transaction ID
	tx: u32,

	/// Transaction amount in major currency units
	amount: Option<f64>,
}

/// A row of the output CSV file
#[derive(Serialize)]
struct OutRow {
	/// Client ID
	client: u16,

	/// Available amount in major currency units
	available: String,

	/// Held amount in major currency units
	held: String,

	/// Total amount in major currency units
	total: String,

	/// Account locked due to a chargeback. No more withdrawals are possible.
	locked: bool,
}

/// Supported transactions types
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum TxType {
	Deposit,
	Withdrawal,
	Dispute,
	Resolve,
	Chargeback,
}

// State of a possibly initiated dispute for a deposit transaction
enum DisputeState {
	NotInitiated,
	Initiated,
	ChargedBack,
}

/// Deposit transaction state and amount.
/// Stored for dispute resolution purposes only.
struct Deposit {
	// State of a possibly initiated dispute for the transaction
	dispute_state: DisputeState,

	/// Transaction amount in minor units.
	amount: i64,
}

/// Current state of a client's account
#[derive(Default)]
struct Account {
	/// Account locked due to a chargeback. No more withdrawals are possible.
	locked: bool,

	/// Funds currently available for withdrawal in minor currency units
	available: i64,

	/// Funds currently held from withdrawal in minor currency units
	held: i64,

	/// Deposit transaction registry by transaction ID
	deposits: HashMap<u32, Deposit>,
}

/// Convert amount in major currency units to minor units.
///
/// Done to avoid FP arithmetic errors.
/// There are 10_000 minor in each major unit of currency.
///
/// If arbitrary precisions is desired, these can be switched to bignums later
/// on. Not used at the moment, as ints are more efficient.
fn to_minor(amount: f64) -> i64 {
	(amount * 10_000_f64) as _
}

/// Convert amount in minor currency units to a major unit string of 4 decimal
/// precision
fn to_major(amount: i64) -> String {
	format!("{:.4}", (amount as f64) / 10_000_f64)
}

#[cfg(test)]
mod test {
	use std::io::Cursor;

	use crate::process;

	/// Load input sample and expected output
	macro_rules! load_samples {
		($dir:literal) => {{
			(
				include_str!(concat!("../test_samples/", $dir, "/in.csv")),
				include_str!(concat!("../test_samples/", $dir, "/out.csv")),
			)
		}};
	}

	// Simple case of deposits and withdrawals
	#[test]
	fn simple() {
		let (input, expected) = load_samples!("simple");
		compare(input, expected);
	}

	// Deposits, withdrawals and dispute resolution
	#[test]
	fn disputes() {
		let (input, expected) = load_samples!("disputes");
		compare(input, expected);
	}

	fn compare(input: &str, expected: &str) {
		let mut res = vec![];
		process(&mut Cursor::new(&mut res), &mut Cursor::new(input)).unwrap();

		fn sort(csv: &str) -> String {
			let i = csv.find('\n').unwrap();
			let mut lines = csv[i + 1..].lines().collect::<Vec<_>>();
			lines.sort();
			lines.iter().fold(csv[..i].to_owned(), |mut w, line| {
				w.push('\n');
				w += line;
				w
			})
		}

		assert_eq!(sort(expected), sort(&String::from_utf8(res).unwrap()));
	}
}
