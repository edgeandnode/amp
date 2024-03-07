pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type Bytes = Box<[u8]>;
pub type EvmAddress = [u8; 20];

// Payment amount in the EVM. Used for gas or value transfers.
pub type EvmCurrency = i128;
