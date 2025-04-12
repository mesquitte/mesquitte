#[cfg(not(any(feature = "v4", feature = "v5")))]
compile_error!("v4 or v5 must be enabled");
#[cfg(not(any(
    feature = "mqtt",
    feature = "mqtts",
    feature = "ws",
    feature = "wss",
    feature = "quic"
)))]
compile_error!("mqtt or mqtts or ws or wss or quic must be enabled");

pub mod broker;
#[cfg(all(feature = "cluster", feature = "rocksdb-storage"))]
pub mod cluster;
pub mod server;
pub mod store;

mod protocols;

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
