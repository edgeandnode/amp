use std::{iter::once, sync::Arc};

use datafusion::arrow::{
    array::{
        BinaryArray, BooleanArray, FixedSizeBinaryBuilder, Int32Array, UInt32Array, UInt64Array,
        UInt64Builder,
    },
    error::ArrowError,
};

use crate::{
    Bytes, Bytes32, Bytes32ArrayType, EvmAddress, EvmCurrency, EvmCurrencyArrayType,
    TimestampSecond, EVM_CURRENCY_TYPE,
};

use super::arrow::array::Array;

pub trait ScalarToArray {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError>;
}

impl ScalarToArray for u64 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(UInt64Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for Bytes32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Bytes32ArrayType::try_from_iter(once(self))?))
    }
}

impl ScalarToArray for Bytes {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(BinaryArray::from_iter_values(once(self))))
    }
}

impl ScalarToArray for Option<u64> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = UInt64Builder::new();
        builder.append_option(*self);
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for EvmAddress {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Bytes32ArrayType::try_from_iter(once(self))?))
    }
}

impl ScalarToArray for Option<Bytes32> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = FixedSizeBinaryBuilder::new(32);
        match self {
            Some(bytes) => builder.append_value(bytes)?,
            None => builder.append_null(),
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for EvmCurrency {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let array =
            EvmCurrencyArrayType::from_iter_values(once(*self)).with_data_type(EVM_CURRENCY_TYPE);
        Ok(Arc::new(array))
    }
}

impl ScalarToArray for Option<EvmCurrency> {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        let mut builder = EvmCurrencyArrayType::builder(1).with_data_type(EVM_CURRENCY_TYPE);
        builder.append_option(*self);
        Ok(Arc::new(builder.finish()))
    }
}

impl ScalarToArray for bool {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(BooleanArray::from_iter(once(Some(*self)))))
    }
}

impl ScalarToArray for u32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(UInt32Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for i32 {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        Ok(Arc::new(Int32Array::from_iter_values(once(*self))))
    }
}

impl ScalarToArray for TimestampSecond {
    fn to_arrow(&self) -> Result<Arc<dyn Array>, ArrowError> {
        use datafusion::arrow::array::TimestampSecondArray;

        let array = TimestampSecondArray::from_iter_values(once(self.0 as i64)).with_timezone_utc();
        Ok(Arc::new(array))
    }
}
