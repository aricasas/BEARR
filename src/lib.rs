mod btree;
mod database;
mod error;
mod eviction;
mod file_system;
mod hash;
mod hashtable;
mod list;
mod memtable;
mod merge;
mod sst;

#[cfg(test)]
mod test_util;

#[cfg(feature = "mock")]
mod mock;

pub use database::{Database, DbConfiguration};
pub use error::DbError;

const PAGE_SIZE: usize = 4096;
