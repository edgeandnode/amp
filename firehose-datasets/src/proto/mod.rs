pub mod google {
    pub mod protobuf {
        include!("google.protobuf.rs");
    }
}
pub mod sf {
    pub mod ethereum {
        pub mod r#type {
            pub mod v2 {
                include!("sf.ethereum.r#type.v2.rs");
            }
        }
    }
    pub mod firehose {
        pub mod v2 {
            include!("sf.firehose.v2.rs");
        }
    }
}
