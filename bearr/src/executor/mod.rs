mod executor;
pub mod io;
pub mod pool;
pub mod sync;

use bearr_error::DbError;
use std::{ops::RangeInclusive, path::PathBuf};

pub use executor::Executor;

use crate::{Database, DbConfiguration};

pub struct DbRequest {
    user_data: u64,
    request: DbOperation,
}
pub enum DbOperation {
    Create {
        name: PathBuf,
        configuration: DbConfiguration,
    },
    Open {
        name: PathBuf,
    },
    Get {
        key: u64,
    },
    Put {
        key: u64,
        value: u64,
    },
    Delete {
        key: u64,
    },
    Flush,
}

pub struct DbResponse {
    user_data: u64,
    response: Result<DbRet, DbError>,
}
pub enum DbRet {
    DbHandle(Database),
    Value(Option<u64>),
    None,
}
