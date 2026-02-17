//! CSV loader for Canton data samples.
//!
//! Provides functionality to load Canton data from gzip-compressed CSV files,
//! such as those in the canton-network-validator data-samples directory.
//!
//! ## Supported Files
//!
//! - `01_transactions.csv.gz` -> transactions table
//! - `03_contracts_created.csv.gz` -> contracts_created table
//! - `04_choices_exercised.csv.gz` -> choices_exercised table
//! - `05_mining_rounds.csv.gz` -> mining_rounds table

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use flate2::read::GzDecoder;
use serde::Deserialize;

use crate::tables::{
    choices_exercised::ChoiceExercised, contracts_created::ContractCreated,
    mining_rounds::MiningRound, transactions::Transaction,
};

/// Errors that can occur when loading CSV files.
#[derive(Debug, thiserror::Error)]
pub enum CsvLoadError {
    /// Failed to open file
    #[error("Failed to open file: {0}")]
    FileOpen(#[from] std::io::Error),
    /// Failed to parse CSV
    #[error("Failed to parse CSV: {0}")]
    CsvParse(#[from] csv::Error),
    /// Invalid file extension
    #[error("Invalid file extension, expected .csv or .csv.gz")]
    InvalidExtension,
}

/// Create a reader that handles both .csv and .csv.gz files.
fn open_csv_reader(path: &Path) -> Result<Box<dyn Read>, CsvLoadError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    // Check if the file is gzip-compressed
    let path_str = path.to_string_lossy();
    if path_str.ends_with(".csv.gz") || path_str.ends_with(".gz") {
        Ok(Box::new(GzDecoder::new(reader)))
    } else if path_str.ends_with(".csv") {
        Ok(Box::new(reader))
    } else {
        Err(CsvLoadError::InvalidExtension)
    }
}

/// Raw CSV record for transactions (01_transactions.csv)
#[derive(Debug, Deserialize)]
struct TransactionCsvRecord {
    offset: String,
    update_id: String,
    record_time: String,
    effective_at: String,
    synchronizer_id: String,
    event_count: u32,
}

/// Load transactions from a CSV file.
///
/// # Arguments
/// * `path` - Path to the CSV file (can be .csv or .csv.gz)
///
/// # Returns
/// A vector of Transaction records
pub fn load_transactions(path: &Path) -> Result<Vec<Transaction>, CsvLoadError> {
    let reader = open_csv_reader(path)?;
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut records = Vec::new();

    for result in csv_reader.deserialize() {
        let record: TransactionCsvRecord = result?;

        // Parse offset from hex string (e.g., "000000000000000001")
        let offset = i64::from_str_radix(record.offset.trim_start_matches("0x"), 16)
            .unwrap_or_else(|_| record.offset.parse().unwrap_or(0));

        records.push(Transaction {
            offset,
            update_id: record.update_id,
            record_time: record.record_time,
            effective_at: if record.effective_at.is_empty() {
                None
            } else {
                Some(record.effective_at)
            },
            synchronizer_id: record.synchronizer_id,
            event_count: record.event_count,
        });
    }

    Ok(records)
}

/// Raw CSV record for contracts_created (03_contracts_created.csv)
#[derive(Debug, Deserialize)]
struct ContractCreatedCsvRecord {
    contract_id: String,
    template_id: String,
    package_name: String,
    created_at: String,
    signatories: String,
    observers: String,
}

/// Load contracts_created from a CSV file.
///
/// # Arguments
/// * `path` - Path to the CSV file (can be .csv or .csv.gz)
/// * `base_block_num` - Base block number to use (records will increment from this)
///
/// # Returns
/// A vector of ContractCreated records
pub fn load_contracts_created(
    path: &Path,
    base_block_num: u64,
) -> Result<Vec<ContractCreated>, CsvLoadError> {
    let reader = open_csv_reader(path)?;
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut records = Vec::new();
    let mut block_num = base_block_num;

    for result in csv_reader.deserialize() {
        let record: ContractCreatedCsvRecord = result?;

        records.push(ContractCreated {
            block_num,
            contract_id: record.contract_id,
            template_id: record.template_id,
            package_name: record.package_name,
            created_at: record.created_at,
            signatories: record.signatories,
            observers: if record.observers.is_empty() {
                None
            } else {
                Some(record.observers)
            },
        });

        block_num += 1;
    }

    Ok(records)
}

/// Raw CSV record for choices_exercised (04_choices_exercised.csv)
#[derive(Debug, Deserialize)]
struct ChoiceExercisedCsvRecord {
    event_id: String,
    contract_id: String,
    template_id: String,
    choice: String,
    consuming: String,
    acting_parties: String,
    effective_at: String,
}

/// Load choices_exercised from a CSV file.
///
/// # Arguments
/// * `path` - Path to the CSV file (can be .csv or .csv.gz)
/// * `base_block_num` - Base block number to use (records will increment from this)
///
/// # Returns
/// A vector of ChoiceExercised records
pub fn load_choices_exercised(
    path: &Path,
    base_block_num: u64,
) -> Result<Vec<ChoiceExercised>, CsvLoadError> {
    let reader = open_csv_reader(path)?;
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut records = Vec::new();
    let mut block_num = base_block_num;

    for result in csv_reader.deserialize() {
        let record: ChoiceExercisedCsvRecord = result?;

        // Parse consuming boolean from string
        let consuming = record.consuming.to_lowercase() == "true";

        records.push(ChoiceExercised {
            block_num,
            event_id: record.event_id,
            contract_id: record.contract_id,
            template_id: record.template_id,
            choice: record.choice,
            consuming,
            acting_parties: record.acting_parties,
            effective_at: record.effective_at,
        });

        block_num += 1;
    }

    Ok(records)
}

/// Raw CSV record for mining_rounds (05_mining_rounds.csv)
#[derive(Debug, Deserialize)]
struct MiningRoundCsvRecord {
    round_number: String,
    contract_id: String,
    amulet_price: String,
    opens_at: String,
    target_closes_at: String,
    tick_duration_us: String,
}

/// Load mining_rounds from a CSV file.
///
/// # Arguments
/// * `path` - Path to the CSV file (can be .csv or .csv.gz)
///
/// # Returns
/// A vector of MiningRound records
pub fn load_mining_rounds(path: &Path) -> Result<Vec<MiningRound>, CsvLoadError> {
    let reader = open_csv_reader(path)?;
    let mut csv_reader = csv::Reader::from_reader(reader);
    let mut records = Vec::new();

    for result in csv_reader.deserialize() {
        let record: MiningRoundCsvRecord = result?;

        let round_number: i64 = record.round_number.parse().unwrap_or(0);
        let amulet_price: f64 = record.amulet_price.parse().unwrap_or(0.0);
        let tick_duration_us: i64 = record.tick_duration_us.parse().unwrap_or(0);

        records.push(MiningRound {
            round_number,
            contract_id: record.contract_id,
            amulet_price,
            opens_at: record.opens_at,
            target_closes_at: record.target_closes_at,
            tick_duration_us,
        });
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_transactions_csv() {
        let csv_data = r#"offset,update_id,record_time,effective_at,synchronizer_id,event_count
000000000000000001,update123,2025-01-01T00:00:00Z,2025-01-01T00:00:01Z,domain1,5
000000000000000002,update456,2025-01-02T00:00:00Z,,domain2,3
"#;

        let mut temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let records = load_transactions(temp_file.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[0].update_id, "update123");
        assert_eq!(records[0].event_count, 5);
        assert_eq!(records[1].offset, 2);
        assert!(records[1].effective_at.is_none());
    }

    #[test]
    fn test_load_mining_rounds_csv() {
        let csv_data = r#"round_number,contract_id,amulet_price,opens_at,target_closes_at,tick_duration_us
0,contract123,0.0700000000,2025-01-01T00:00:00Z,2025-01-01T02:00:00Z,600000000
1,contract456,0.0500000000,2025-01-01T02:00:00Z,2025-01-01T04:00:00Z,600000000
"#;

        let mut temp_file = NamedTempFile::with_suffix(".csv").unwrap();
        temp_file.write_all(csv_data.as_bytes()).unwrap();

        let records = load_mining_rounds(temp_file.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].round_number, 0);
        assert!((records[0].amulet_price - 0.07).abs() < 0.0001);
        assert_eq!(records[1].round_number, 1);
    }
}
