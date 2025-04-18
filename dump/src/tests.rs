use crate::resolve_relative_block_range;
use common::{BlockNum, BoxError};

#[test]
fn test_validate_block_range() {
    let test_cases: Vec<(i64, Option<i64>, Result<(BlockNum, BlockNum), BoxError>)> = vec![
        (50, None, Ok((50, 100))),
        (-80, None, Ok((20, 100))),
        (100, Some(150), Ok((100, 150))),
        (-80, Some(-10), Ok((20, 90))),
        (70, Some(-50), Err(BoxError::from(""))), // overlapped
    ];

    for (start_block, end_block, expected) in test_cases {
        match resolve_relative_block_range(start_block, end_block, 100) {
            Ok(result) => assert_eq!(expected.unwrap(), result),
            Err(_) => assert!(expected.is_err()),
        }
    }
}
use crate::resolve_relative_block_range;
use common::{BlockNum, BoxError};

#[test]
fn test_validate_block_range() {
    let test_cases: Vec<(i64, Option<i64>, Result<(BlockNum, BlockNum), BoxError>)> = vec![
        (50, None, Ok((50, 100))),
        (-80, None, Ok((20, 100))),
        (100, Some(150), Ok((100, 150))),
        (-80, Some(-10), Ok((20, 90))),
        (70, Some(-50), Err(BoxError::from(""))), // overlapped
    ];

    for (start_block, end_block, expected) in test_cases {
        match resolve_relative_block_range(start_block, end_block, 100) {
            Ok(result) => assert_eq!(expected.unwrap(), result),
            Err(_) => assert!(expected.is_err()),
        }
    }
}