use std::{
    fs::{self, File},
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    lsm::{LsmConfiguration, LsmMetadata, LsmTree},
};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

/// An open connection to a database
pub struct Database {
    name: PathBuf,
    lsm: LsmTree,
    file_system: FileSystem,
}

/// Configuration options for a database
#[derive(Serialize, Deserialize)]
pub struct DbConfiguration {
    pub lsm_configuration: LsmConfiguration,
    pub buffer_pool_capacity: usize,
    pub write_buffering: usize,
}

impl DbConfiguration {
    fn validate(&self) -> Result<(), DbError> {
        self.lsm_configuration.validate()?;
        if self.buffer_pool_capacity >= 16 && self.write_buffering > 0 {
            Ok(())
        } else {
            Err(DbError::InvalidConfiguration)
        }
    }
}

/// Metadata for a database
#[derive(Serialize, Deserialize)]
struct DbMetadata {
    lsm_metadata: LsmMetadata,
}

const CONFIG_FILENAME: &str = "config.json";
const METADATA_FILENAME: &str = "metadata.json";

impl Database {
    /// Creates and returns an empty database, initializing a folder with the given path.
    ///
    /// Returns `DbError::IoError` if:
    /// - The path already exists.
    /// - A parent of the path does not exist.
    /// - There are problems with creating/writing to files.
    ///
    /// May return other errors if creation of the memtable or SSTs fails.
    pub fn create(name: impl AsRef<Path>, configuration: DbConfiguration) -> Result<Self, DbError> {
        let name = name.as_ref();
        fs::create_dir(name)?;

        let config_file = File::create_new(name.join(CONFIG_FILENAME))?;
        serde_json::to_writer_pretty(config_file, &configuration)?;

        let metadata = DbMetadata {
            lsm_metadata: LsmMetadata::empty(),
        };
        let metadata_file = File::create_new(name.join(METADATA_FILENAME))?;
        serde_json::to_writer_pretty(metadata_file, &metadata)?;

        Self::new(name, configuration, metadata)
    }

    /// Opens the database located at the given path.
    ///
    /// Returns `DbError::IoError` if:
    /// - The path does not exist.
    /// - There are problems with reading files.
    ///
    /// May return other errors if creation of the memtable or SSTs fails.
    pub fn open(name: impl AsRef<Path>) -> Result<Self, DbError> {
        let name = name.as_ref();

        let config_file = File::open(name.join(CONFIG_FILENAME))?;
        let configuration: DbConfiguration = serde_json::from_reader(config_file)?;

        let metadata_file = File::open(name.join(METADATA_FILENAME))?;
        let metadata: DbMetadata = serde_json::from_reader(metadata_file)?;

        Self::new(name, configuration, metadata)
    }

    /// Returns a database from the given path, configuration, and metadata.
    ///
    /// Returns an error if creation of the memtable or SSTs fails.
    fn new(
        name: &Path,
        configuration: DbConfiguration,
        metadata: DbMetadata,
    ) -> Result<Self, DbError> {
        configuration.validate()?;

        let file_system = FileSystem::new(
            name,
            configuration.buffer_pool_capacity,
            configuration.write_buffering,
        )?;

        let lsm = LsmTree::open(
            metadata.lsm_metadata,
            configuration.lsm_configuration,
            &file_system,
        )?;

        Ok(Self {
            name: name.to_path_buf(),
            lsm,
            file_system,
        })
    }

    /// Returns the value associated with the given key, if it exists.
    ///
    /// Returns an error if searching fails in an SST.
    pub fn get(&self, key: u64) -> Result<Option<u64>, DbError> {
        self.lsm.get(key, &self.file_system)
    }

    /// Inserts the given key-value pair into the database,
    /// flushing the memtable if it reaches capacity.
    ///
    /// Returns an error if flushing fails. See `Database::flush` for more info.
    pub fn put(&mut self, key: u64, value: u64) -> Result<(), DbError> {
        self.lsm.put(key, value, &mut self.file_system)
    }

    /// TODO: Documentation
    pub fn delete(&mut self, key: u64) -> Result<(), DbError> {
        self.lsm.delete(key, &mut self.file_system)
    }

    /// Returns a sorted list of all key-value pairs where the key is in the given range.
    ///
    /// Returns an error if scanning fails in the memtable or SSTs.
    pub fn scan(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64), DbError>>, DbError> {
        self.lsm.scan(range, &self.file_system)
    }

    /// Transforms the current memtable into an SST, if the current memtable is nonempty.
    ///
    /// The new SST is saved to a file, and a new memtable is created to replace it.
    ///
    /// Returns an error if:
    /// - Scanning the memtable fails.
    /// - Creation of the new SST fails.
    /// - Creation of the new memtable fails.
    pub fn flush(&mut self) -> Result<(), DbError> {
        self.lsm.flush_memtable(&mut self.file_system)?;
        let lsm_metadata = self.lsm.metadata();
        let metadata = DbMetadata { lsm_metadata };
        let metadata_file = File::create(self.name.join(METADATA_FILENAME))?;
        serde_json::to_writer_pretty(metadata_file, &metadata)?;

        Ok(())
    }
}

/// The database is flushed upon dropping.
///
/// Errors are ignored. To handle them, call `Database::flush` manually.
impl Drop for Database {
    fn drop(&mut self) {
        _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::test_util::TestPath;

    use super::*;

    fn test_path(name: &str) -> TestPath {
        TestPath::new("database", name)
    }

    fn put_many(db: &mut Database, pairs: &[(u64, u64)]) -> Result<()> {
        for &(k, v) in pairs {
            db.put(k, v)?;
        }
        Ok(())
    }

    fn assert_pairs(db: &Database, pairs: &[(u64, Option<u64>)]) -> Result<()> {
        for &(k, v) in pairs {
            assert_eq!(db.get(k)?, v);
        }
        Ok(())
    }

    #[test]
    fn test_basic() -> Result<()> {
        let name = &test_path("basic");
        let mut db = Database::create(
            name,
            DbConfiguration {
                buffer_pool_capacity: 16,
                write_buffering: 1,
                lsm_configuration: LsmConfiguration {
                    size_ratio: 2,
                    memtable_capacity: 3,
                },
            },
        )?;

        put_many(
            &mut db,
            &[
                (3, 1),
                (4, 1),
                (9, 5),
                (2, 6),
                (5, 3),
                (8, 5),
                (97, 9),
                (32, 3),
                (84, 6),
            ],
        )?;

        assert_pairs(
            &db,
            &[
                (3, Some(1)),
                (4, Some(1)),
                (9, Some(5)),
                (2, Some(6)),
                (5, Some(3)),
                (8, Some(5)),
                (97, Some(9)),
                (32, Some(3)),
                (84, Some(6)),
                (1, None),
                (10, None),
                (90, None),
            ],
        )?;

        assert_eq!(
            db.scan(4..=84)?.collect::<Result<Vec<_>, _>>()?,
            vec![(4, 1), (5, 3), (8, 5), (9, 5), (32, 3), (84, 6)]
        );

        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let name = &test_path("persistence");

        {
            let mut db = Database::create(
                name,
                DbConfiguration {
                    buffer_pool_capacity: 16,
                    write_buffering: 1,
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 2,
                        memtable_capacity: 10,
                    },
                },
            )?;
            put_many(&mut db, &[(13, 15), (14, 1), (4, 19)])?;
        }

        {
            let mut db = Database::open(name)?;
            assert_pairs(&db, &[(13, Some(15)), (14, Some(1)), (4, Some(19))])?;
            put_many(&mut db, &[(13, 15), (14, 15), (1, 19)])?;
            db.flush()?;
            put_many(&mut db, &[(3, 1), (20, 5), (7, 15), (18, 25)])?;
            db.flush()?;
        }

        {
            let mut db = Database::open(name)?;

            assert_pairs(
                &db,
                &[
                    (13, Some(15)),
                    (14, Some(15)),
                    (4, Some(19)),
                    (1, Some(19)),
                    (3, Some(1)),
                    (20, Some(5)),
                    (7, Some(15)),
                    (18, Some(25)),
                ],
            )?;

            put_many(&mut db, &[(5, 14), (4, 15)])?;
            db.flush()?;
            put_many(&mut db, &[(6, 21), (14, 3), (20, 15), (18, 19)])?;

            assert_pairs(
                &db,
                &[
                    (13, Some(15)),
                    (14, Some(3)),
                    (4, Some(15)),
                    (1, Some(19)),
                    (3, Some(1)),
                    (20, Some(15)),
                    (7, Some(15)),
                    (18, Some(19)),
                    (5, Some(14)),
                    (6, Some(21)),
                ],
            )?;

            assert_eq!(
                db.scan(5..=15)?.collect::<Result<Vec<_>, _>>()?,
                vec![(5, 14), (6, 21), (7, 15), (13, 15), (14, 3)]
            );
        }

        Ok(())
    }
}
