#[cfg(feature = "rocksdb-storage")]
pub mod rocksdb;
#[cfg(feature = "rocksdb-storage")]
pub use rocksdb::{
    log_store,
    store::{Request, Response, StateMachineData, StateMachineStore, new},
};
