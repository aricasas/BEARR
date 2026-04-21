mod executor;
pub mod io;
pub mod sync;

use bearr_error::DbError;
use std::ops::RangeInclusive;

pub use executor::Executor;

pub enum DbRequest {
    Get { key: u64 },
    Put { key: u64, value: u64 },
    Delete { key: u64 },
    Scan { range: RangeInclusive<u64> },
    Flush,
}

pub enum DbResponse {
    Get {
        result: Result<Option<u64>, DbError>,
    },
    Put {
        result: Result<(), DbError>,
    },
    Delete {
        result: Result<(), DbError>,
    },
}
