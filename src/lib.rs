mod btree;
mod database;
mod db_scan;
mod error;
mod eviction;
mod file_system;
mod hashtable;
mod list;
mod memtable;
mod merge;
mod sst;

#[cfg(feature = "mock")]
mod mock;

pub use database::{Database, DbConfiguration};
pub use error::DbError;

const PAGE_SIZE: usize = 4096;
