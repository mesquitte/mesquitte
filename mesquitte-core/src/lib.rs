mod protocols;

#[cfg(all(
    feature = "cluster",
    any(
        all(feature = "heed-storage", not(feature = "rocksdb-storage")),
        all(feature = "rocksdb-storage", not(feature = "heed-storage"))
    )
))]
pub mod cluster;
pub mod server;
pub mod store;

#[macro_export]
macro_rules! trace { ($($x:tt)*) => (
    #[cfg(feature = "log")] {
        log::trace!($($x)*)
    }
) }

#[macro_export]
macro_rules! debug { ($($x:tt)*) => (
    #[cfg(feature = "log")] {
        log::debug!($($x)*)
    }
) }

#[macro_export]
macro_rules! info { ($($x:tt)*) => (
    #[cfg(feature = "log")] {
        log::info!($($x)*)
    }
) }

#[macro_export]
macro_rules! warn { ($($x:tt)*) => (
    #[cfg(feature = "log")] {
        log::warn!($($x)*)
    }
) }

#[macro_export]
macro_rules! error { ($($x:tt)*) => (
    #[cfg(feature = "log")] {
        log::error!($($x)*)
    }
) }
