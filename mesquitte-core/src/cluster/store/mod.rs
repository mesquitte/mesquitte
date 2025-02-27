#[cfg(all(feature = "heed-storage", not(feature = "rocksdb-storage")))]
pub mod heed;
#[cfg(all(feature = "heed-storage", not(feature = "rocksdb-storage")))]
pub use heed::{
    log_store,
    store::{Request, Response, StateMachineData, StateMachineStore, new},
};
#[cfg(all(feature = "rocksdb-storage", not(feature = "heed-storage")))]
pub mod rocksdb;
#[cfg(all(feature = "rocksdb-storage", not(feature = "heed-storage")))]
pub use rocksdb::{
    log_store,
    store::{Request, Response, StateMachineData, StateMachineStore, new},
};
