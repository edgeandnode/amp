use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{stats::Precision, Column, ColumnStatistics, SchemaError::FieldNotFound, Statistics},
    error::{DataFusionError, Result as DataFusionResult},
    physical_expr::utils::Guarantee,
    scalar::ScalarValue,
};

use crate::{metadata::MetadataHash, BlockNum, BLOCK_NUM, SPECIAL_BLOCK_NUM};

pub(super) type PruningGuarantees =
    HashMap<Guarantee, HashMap<Arc<str>, HashSet<Arc<ScalarValue>>>>;

pub(crate) type RowGroupOrdinal = u64;

#[derive(Debug, Clone, Eq, Ord, PartialEq, Hash)]
pub struct RowGroupId(
    /// The scanned range start block number for a file.
    pub BlockNum,
    /// The ordinal of the row group within the file.
    pub RowGroupOrdinal,
);

impl From<(BlockNum, RowGroupOrdinal)> for RowGroupId {
    fn from((block_num, ordinal): (BlockNum, RowGroupOrdinal)) -> Self {
        Self(block_num, ordinal)
    }
}

impl PartialOrd for RowGroupId {
    fn ge(&self, other: &Self) -> bool {
        self.0 >= other.0 && self.1 >= other.1
    }
    fn gt(&self, other: &Self) -> bool {
        self.0 > other.0 || (self.0 == other.0 && self.1 > other.1)
    }
    fn le(&self, other: &Self) -> bool {
        self.0 <= other.0 && self.1 <= other.1
    }
    fn lt(&self, other: &Self) -> bool {
        self.0 < other.0 || (self.0 == other.0 && self.1 < other.1)
    }
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else if self > other {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct RowGroupStatisticsCache {
    inner: BTreeMap<RowGroupId, (Arc<Statistics>, MetadataHash)>,
}

// methods to provide CacheAccessor-like functionality for RowGroupStatisticsCache
impl RowGroupStatisticsCache {
    /// Get `Statistics` for specified row_group.
    fn get(&self, k: &RowGroupId) -> Option<Arc<Statistics>> {
        self.inner
            .get(k)
            .map(|(statistics, ..)| Arc::clone(statistics))
    }

    /// Get `Statistics` for specified row_group.
    /// Returns None if `Statistics` have changed or not found.
    fn get_with_extra(&self, k: &RowGroupId, e: &MetadataHash) -> Option<Arc<Statistics>> {
        self.inner
            .get(k)
            .map(|(statistics, hash)| {
                if hash == e {
                    Some(Arc::clone(statistics))
                } else {
                    None
                }
            })
            .flatten()
    }

    fn put_with_extra(
        &mut self,
        key: &RowGroupId,
        value: Arc<Statistics>,
        e: &MetadataHash,
    ) -> Option<Arc<Statistics>> {
        self.inner
            .insert(key.clone(), (value, e.clone()))
            .map(|(statistics, ..)| statistics)
    }

    fn remove(&mut self, k: &RowGroupId) -> Option<Arc<Statistics>> {
        self.inner.remove(k).map(|(statistics, ..)| statistics)
    }

    fn contains_key(&self, k: &RowGroupId) -> bool {
        self.inner.contains_key(k)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&mut self) {
        self.inner.clear()
    }

    fn name(&self) -> String {
        "RowGroupStatisticsCache".to_string()
    }
}

pub(super) fn statistics_converter<T, U: std::fmt::Debug + Clone + Eq + PartialOrd>(
    val: T,
    is_exact: bool,
    f: impl Fn(T) -> U,
) -> Precision<U> {
    let precision = if is_exact {
        Precision::Exact
    } else {
        Precision::Inexact
    };
    precision(f(val))
}

pub(super) fn convert<T, U: std::fmt::Debug + Clone + Eq + PartialOrd>(
    is_exact: bool,
    f: impl Fn(T) -> U,
) -> impl FnOnce(T) -> Precision<U> {
    move |val: T| {
        let precision = if is_exact {
            Precision::Exact
        } else {
            Precision::Inexact
        };
        precision(f(val))
    }
}

pub(super) fn update_statistics<T>(
    column: &mut ColumnStatistics,
    distinct_count_opt: Option<u64>,
    null_count_opt: Option<u64>,
    max_opt: Option<T>,
    min_opt: Option<T>,
    max_is_exact: bool,
    min_is_exact: bool,
    ref f: impl Fn(T) -> ScalarValue,
) {
    column.distinct_count = distinct_count_opt
        .map(|val| statistics_converter(&val, true, |v| *v as usize))
        .unwrap_or_default();
    column.null_count = null_count_opt
        .map(|val| statistics_converter(&val, true, |v| *v as usize))
        .unwrap_or_default();
    column.max_value = max_opt.map(convert(max_is_exact, f)).unwrap_or_default();
    column.min_value = min_opt.map(convert(min_is_exact, f)).unwrap_or_default();
}

pub(super) fn determine_pruning(
    guarantee: Guarantee,
    guarantees: Arc<PruningGuarantees>,
    statistics: &Statistics,
    schema: &SchemaRef,
    mut prune: bool,
) {
    let value = match guarantee {
        Guarantee::In => false,
        Guarantee::NotIn => true,
    };
    if let Some(in_guarantees) = guarantees.get(&Guarantee::In) {
        in_guarantees.iter().for_each(|(name, literals)| {
            let idx = schema.index_of(name).unwrap();
            let column_statistics = &statistics.column_statistics[idx];
            if let Some(min_value) = column_statistics.min_value.get_value() {
                if let Some(max_value) = column_statistics.max_value.get_value() {
                    if literals.iter().any(|literal| {
                        literal.as_ref() >= min_value && literal.as_ref() <= max_value
                    }) {
                        prune = value;
                    }
                }
            }
        });
    }
}

pub(super) fn get_min_block_num(
    statistics: &Statistics,
    schema: &SchemaRef,
    table_name: &str,
) -> DataFusionResult<u64> {
    let idx = if let Some((idx, _)) = schema.column_with_name(BLOCK_NUM) {
        idx
    } else if let Some((idx, _)) = schema.column_with_name(SPECIAL_BLOCK_NUM) {
        idx
    } else {
        let name = format!("{} or {}", BLOCK_NUM, SPECIAL_BLOCK_NUM);
        let field = Column::from_name(name).into();
        let valid_fields = Vec::new();
        return Err(DataFusionError::SchemaError(
            FieldNotFound {
                field,
                valid_fields,
            },
            Box::new(Some(format!(
                "Table {} does not have a column named `{}` or `{}`",
                table_name, BLOCK_NUM, SPECIAL_BLOCK_NUM,
            ))),
        ));
    };

    // Unwrap: We just checked that the column exists
    // Unwrap: We just set the statistics for this column
    let min_block_num = statistics
        .column_statistics
        .get(idx)
        .unwrap()
        .min_value
        .get_value()
        .unwrap()
        .clone();

    match min_block_num {
        ScalarValue::UInt64(Some(num)) => Ok(num),
        // Just in case the column is Int64, we convert it to u64
        ScalarValue::Int64(Some(num)) => Ok(num as u64),
        _ => Err(DataFusionError::Internal(format!(
            "Expected columns `{}` or `{}` to be of type UInt64 or Int64, but found {:?}",
            BLOCK_NUM, SPECIAL_BLOCK_NUM, min_block_num
        ))),
    }
}
