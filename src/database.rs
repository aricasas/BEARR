use std::{
    fs::{self, File},
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    lsm::{LsmConfiguration, LsmMetadata, LsmTree, TOMBSTONE},
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
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
#[derive(Debug, Serialize, Deserialize)]
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
        if value == TOMBSTONE {
            return Err(DbError::InvalidValue);
        }

        self.lsm.put(key, value, &self.file_system)
    }

    /// Removes the key-value pair with given key from the database, if one exists.
    ///
    /// Has no effect on the set of key-value pairs in the database
    /// if the pair with the given key does not exist,
    /// but may affect how the data is internally stored.
    ///
    /// Returns an error if deletion fails.
    pub fn delete(&mut self, key: u64) -> Result<(), DbError> {
        self.lsm.delete(key, &self.file_system)
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
        self.lsm.flush_memtable(&self.file_system)?;
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
    use std::{collections::HashMap, ops::Range};

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
            assert_eq!(db.get(k)?, v, "key {k}");
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
                    bloom_filter_bits: 1,
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
                        bloom_filter_bits: 2,
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

    #[derive(Debug)]
    enum Command {
        Get,
        Put,
        Delete,
        Scan,
        Flush,
        Restart,
    }

    #[test]
    fn test_chaotic() -> Result<()> {
        const KEY_RANGE: Range<u64> = 0..65536;
        const VALUE_RANGE: Range<u64> = 0..65536;

        let name = &test_path("chaotic");
        let mut db = Some(Database::create(
            name,
            DbConfiguration {
                buffer_pool_capacity: 64,
                write_buffering: 8,
                lsm_configuration: LsmConfiguration {
                    size_ratio: 3,
                    memtable_capacity: 256,
                    bloom_filter_bits: 4,
                },
            },
        )?);
        let mut oracle = HashMap::new();
        for i in 0..256 {
            let command = fastrand::choice([
                Command::Get,
                Command::Put,
                Command::Delete,
                Command::Scan,
                Command::Flush,
                Command::Restart,
            ])
            .unwrap();
            match command {
                Command::Get => {
                    let db = db.as_ref().unwrap();
                    let key = fastrand::u64(KEY_RANGE);
                    println!("{i}. get {key}");
                    assert_eq!(db.get(key)?, oracle.get(&key).copied());
                }
                Command::Put => {
                    let db = db.as_mut().unwrap();
                    let key = fastrand::u64(KEY_RANGE);
                    let value = fastrand::u64(VALUE_RANGE);
                    println!("{i}. put {key} => {value}");
                    db.put(key, value)?;
                    oracle.insert(key, value);
                }
                Command::Delete => {
                    let db = db.as_mut().unwrap();
                    let key = fastrand::u64(KEY_RANGE);
                    println!("{i}. delete {key}");
                    db.delete(key)?;
                    oracle.remove(&key);
                }
                Command::Scan => {
                    let db = db.as_ref().unwrap();
                    let a = fastrand::u64(KEY_RANGE);
                    let b = fastrand::u64(KEY_RANGE);
                    let start = u64::min(a, b);
                    let end = u64::max(a, b);
                    println!("{i}. scan {start}..={end}");
                    let scan = db.scan(start..=end)?.collect::<Result<Vec<_>, _>>()?;
                    let mut oracle_scan: Vec<_> = (start..=end)
                        .filter_map(|key| oracle.get(&key).map(|&value| (key, value)))
                        .collect();
                    oracle_scan.sort_unstable();
                    assert_eq!(scan, oracle_scan);
                }
                Command::Flush => {
                    println!("{i}. flush");
                    let db = db.as_mut().unwrap();
                    db.flush()?;
                }
                Command::Restart => {
                    println!("{i}. restart");
                    let old_handle = db.take().unwrap();
                    drop(old_handle);
                    db = Some(Database::open(name)?);
                }
            }
        }
        Ok(())
    }
}
