mod executor;
pub mod io;
pub mod pool;
pub mod sync;
pub mod tokio;

use bearr_error::DbError;
use std::{path::PathBuf, sync::Arc};

pub use executor::Executor;

use crate::{DbConfiguration, database::Database};

pub struct DbRequest {
    request_id: u64,
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

    RegisterDb {
        database: Arc<Database>,
    },
}

pub struct DbResponse {
    request_id: u64,
    response: Result<DbRet, DbError>,
}
pub enum DbRet {
    DbHandle(Database),
    Value(Option<u64>),
    None,
}
