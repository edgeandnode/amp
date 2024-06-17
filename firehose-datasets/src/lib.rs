//! EVM block data is sufficiently complicated that there may be multiple encoding flavors, each with
//! multiple versions. There is no universal encoding, and we're not going to try to enforce one.
//! Each extraction layer can have its own data format. This `firehose` crate defines Firehose
//! data formats and provides a client to fetch them from a Firehose gRPC endpoint.

pub mod client;
pub mod evm;
pub mod provider;

mod proto;
