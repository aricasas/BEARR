use std::{
    fs::{self, File},
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    file_system::FileSystem,
    lsm::{LsmConfiguration, LsmMetadata, LsmTree, TOMBSTONE},
};

/// An open connection to a database.
pub struct Database {
    name: PathBuf,
    lsm: LsmTree,
    file_system: FileSystem,
}

/// Configuration options for a database.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct DbConfiguration {
    /// Configuration options for the LSM tree.
    pub lsm_configuration: LsmConfiguration,
    /// Number of pages that the buffer pool can hold.
    /// Must be at least 16.
    pub buffer_pool_capacity: usize,
    /// When writing multiple pages to a file,
    /// the number of pages to buffer before issuing an I/O call.
    /// Must be nonzero.
    pub write_buffering: usize,
    /// The number of pages to read sequentially from a file.
    /// Must be nonzero.
    pub readahead_buffering: usize,
}

impl DbConfiguration {
    fn validate(&self) -> Result<(), DbError> {
        self.lsm_configuration.validate()?;
        if self.buffer_pool_capacity >= 16
            && self.write_buffering > 0
            && self.readahead_buffering > 0
        {
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
    /// Creates and returns an empty database with the given configuration,
    /// initializing a folder with the given path.
    ///
    /// Returns `DbError::InvalidConfiguration` if the given configuration
    /// does not meet all of the documented requirements.
    ///
    /// Returns `DbError::IoError` if:
    /// - The path already exists.
    /// - A parent of the path does not exist.
    /// - There are problems with creating/writing to files.
    ///
    /// Also returns errors if creation of the file system struct or LSM tree fails.
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
    /// - The configuration and/or metadata files do not exist at the path.
    /// - There are problems with reading files.
    ///
    /// Also returns errors if creation of the file system struct or LSM tree fails.
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
    /// Returns an error if creation of the file system struct or LSM tree fails.
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
            configuration.readahead_buffering,
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
    /// Returns `DbError::InvalidValue` if the given value is `u64::MAX`
    /// (which is reserved for tombstones).
    ///
    /// Returns an error if flushing fails.
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
    /// Returns `DbError::InvalidScanRange` if `range.start() > range.end()`.
    ///
    /// Returns an error if scanning fails in the memtable or SSTs.
    pub fn scan(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64), DbError>>, DbError> {
        self.lsm.scan(range, &self.file_system)
    }

    /// Transforms the current memtable into an SST, if the current memtable is nonempty.
    /// The new SST is added to the top level of the LSM tree,
    /// and then the levels of the LSM tree may be compacted.
    /// Also saves the current metadata of the LSM tree to a file.
    ///
    /// Returns an error if:
    /// - Scanning the memtable fails.
    /// - Compaction fails in the LSM tree.
    /// - Writing the LSM metadata fails.
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
    use std::{collections::HashMap, ops::Range, thread};

    use anyhow::Result;

    use crate::test_util::TestPath;

    use super::*;

    fn test_path(name: &str) -> TestPath {
        TestPath::create("database", name)
    }

    fn put_many(db: &mut Database, pairs: &[(u64, u64)]) -> Result<()> {
        for &(k, v) in pairs {
            db.put(k, v)?;
        }
        Ok(())
    }

    fn delete_many(db: &mut Database, keys: &[u64]) -> Result<()> {
        for &k in keys {
            db.delete(k)?;
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
                readahead_buffering: 1,
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

        delete_many(&mut db, &[9, 8, 84, 8, 90])?;

        assert_pairs(
            &db,
            &[
                (3, Some(1)),
                (4, Some(1)),
                (9, None),
                (2, Some(6)),
                (5, Some(3)),
                (8, None),
                (97, Some(9)),
                (32, Some(3)),
                (84, None),
                (1, None),
                (10, None),
                (90, None),
            ],
        )?;

        assert_eq!(
            db.scan(4..=32)?.collect::<Result<Vec<_>, _>>()?,
            vec![(4, 1), (5, 3), (32, 3)]
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
                    readahead_buffering: 1,
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
            delete_many(&mut db, &[3, 1, 4, 1, 5, 9])?;
            db.flush()?;
        }

        {
            let mut db = Database::open(name)?;

            assert_pairs(
                &db,
                &[
                    (13, Some(15)),
                    (14, Some(15)),
                    (20, Some(5)),
                    (7, Some(15)),
                    (18, Some(25)),
                ],
            )?;

            put_many(&mut db, &[(5, 14), (4, 15)])?;
            delete_many(&mut db, &[7, 14])?;
            db.flush()?;
            put_many(&mut db, &[(6, 21), (14, 3), (20, 15), (18, 19)])?;

            assert_pairs(
                &db,
                &[
                    (13, Some(15)),
                    (14, Some(3)),
                    (20, Some(15)),
                    (18, Some(19)),
                    (5, Some(14)),
                    (6, Some(21)),
                ],
            )?;

            assert_eq!(
                db.scan(5..=15)?.collect::<Result<Vec<_>, _>>()?,
                vec![(5, 14), (6, 21), (13, 15), (14, 3)]
            );
        }

        Ok(())
    }

    fn create_db(
        name: &str,
        size_ratio: usize,
        memtable_capacity: usize,
        bloom_filter_bits: usize,
        buffer_pool_capacity: usize,
        write_buffering: usize,
        readahead_buffering: usize,
    ) -> Result<Database, DbError> {
        Database::create(
            test_path(name),
            DbConfiguration {
                lsm_configuration: LsmConfiguration {
                    size_ratio,
                    memtable_capacity,
                    bloom_filter_bits,
                },
                buffer_pool_capacity,
                write_buffering,
                readahead_buffering,
            },
        )
    }

    #[test]
    fn test_errors() -> Result<()> {
        let mut db = create_db("errors", 2, 1, 0, 16, 1, 1)?;

        assert_eq!(
            create_db("errors_bad_size_ratio", 1, 1, 0, 16, 1, 1).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_memtable_capacity", 2, 0, 0, 16, 1, 1).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_small_buffer_pool_capacity", 2, 1, 0, 15, 1, 1).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_write_buffering", 2, 1, 0, 16, 0, 1).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_readahead_buffering", 2, 1, 0, 16, 1, 0).err(),
            Some(DbError::InvalidConfiguration)
        );

        assert_eq!(db.put(0, TOMBSTONE), Err(DbError::InvalidValue));

        #[allow(clippy::reversed_empty_ranges)]
        let r = 1..=0;
        assert_eq!(db.scan(r).err(), Some(DbError::InvalidScanRange));

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

    const KEY_RANGE: Range<u64> = 0..65536;
    const VALUE_RANGE: Range<u64> = 0..65536;

    #[test]
    fn test_chaotic() -> Result<()> {
        let name = &test_path("chaotic");
        let mut db = Some(Database::create(
            name,
            DbConfiguration {
                buffer_pool_capacity: 64,
                write_buffering: 8,
                readahead_buffering: 8,
                lsm_configuration: LsmConfiguration {
                    size_ratio: 3,
                    memtable_capacity: 256,
                    bloom_filter_bits: 4,
                },
            },
        )?);
        let mut oracle = HashMap::new();

        for i in 0..4096 {
            let command = fastrand::choice([
                Command::Get,
                Command::Put,
                Command::Delete,
                Command::Scan,
                Command::Flush,
                Command::Restart,
            ])
            .unwrap();
            let command_description;

            match command {
                Command::Get => {
                    let db = db.as_ref().unwrap();
                    let key = if fastrand::f64() < 0.9
                        && let Some(&k) = fastrand::choice(oracle.keys())
                    {
                        k
                    } else {
                        fastrand::u64(KEY_RANGE)
                    };
                    let value = db.get(key)?;
                    assert_eq!(value, oracle.get(&key).copied());
                    command_description = format!("get {key} ==> {value:?}");
                }
                Command::Put => {
                    let db = db.as_mut().unwrap();
                    let key = if fastrand::f64() < 0.1
                        && let Some(&k) = fastrand::choice(oracle.keys())
                    {
                        k
                    } else {
                        fastrand::u64(KEY_RANGE)
                    };
                    let value = fastrand::u64(VALUE_RANGE);
                    command_description = format!("put ({key}, {value})");
                    db.put(key, value)?;
                    oracle.insert(key, value);
                }
                Command::Delete => {
                    let db = db.as_mut().unwrap();
                    let key = if fastrand::f64() < 0.9
                        && let Some(&k) = fastrand::choice(oracle.keys())
                    {
                        k
                    } else {
                        fastrand::u64(KEY_RANGE)
                    };
                    command_description = format!("delete {key}");
                    db.delete(key)?;
                    oracle.remove(&key);
                }
                Command::Scan => {
                    let db = db.as_ref().unwrap();
                    let a = fastrand::u64(KEY_RANGE);
                    let b = fastrand::u64(KEY_RANGE);
                    let start = u64::min(a, b);
                    let end = u64::max(a, b);
                    let scan = db.scan(start..=end)?.collect::<Result<Vec<_>, _>>()?;
                    let mut oracle_scan: Vec<_> = oracle
                        .iter()
                        .filter_map(|(&key, &value)| {
                            (start..=end).contains(&key).then_some((key, value))
                        })
                        .collect();
                    oracle_scan.sort_unstable();
                    assert_eq!(scan, oracle_scan);
                    command_description = format!("scan {start}..={end} ==> # = {}", scan.len());
                }
                Command::Flush => {
                    command_description = "flush".to_owned();
                    let db = db.as_mut().unwrap();
                    db.flush()?;
                }
                Command::Restart => {
                    command_description = "restart".to_owned();
                    let old_handle = db.take().unwrap();
                    drop(old_handle);
                    db = Some(Database::open(name)?);
                }
            }

            if i % 256 == 0 {
                let state = db
                    .as_ref()
                    .unwrap()
                    .scan(u64::MIN..=u64::MAX)?
                    .collect::<Result<Vec<_>, _>>()?;
                println!("{i}. {command_description}; {state:?}");
            }
        }

        Ok(())
    }

    #[test]
    fn test_concurrency() -> Result<()> {
        let name = &test_path("concurrency");
        let mut db = Database::create(
            name,
            DbConfiguration {
                buffer_pool_capacity: 64,
                write_buffering: 8,
                readahead_buffering: 8,
                lsm_configuration: LsmConfiguration {
                    size_ratio: 3,
                    memtable_capacity: 256,
                    bloom_filter_bits: 4,
                },
            },
        )?;
        let mut oracle = HashMap::new();

        while oracle.len() < 4096 {
            let key = fastrand::u64(KEY_RANGE);
            let value = fastrand::u64(VALUE_RANGE);
            db.put(key, value)?;
            oracle.insert(key, value);
        }

        let db = &db;
        let oracle = &oracle;
        thread::scope(|scope| {
            for i in 0..16 {
                scope.spawn(move || {
                    for j in 0..4096 {
                        let command = fastrand::choice([Command::Get, Command::Scan]).unwrap();

                        let command_description = match command {
                            Command::Get => {
                                let key = if fastrand::f64() < 0.9
                                    && let Some(&k) = fastrand::choice(oracle.keys())
                                {
                                    k
                                } else {
                                    fastrand::u64(KEY_RANGE)
                                };
                                let value = db.get(key).unwrap();
                                assert_eq!(value, oracle.get(&key).copied());
                                format!("get {key} ==> {value:?}")
                            }
                            Command::Scan => {
                                let a = fastrand::u64(KEY_RANGE);
                                let b = fastrand::u64(KEY_RANGE);
                                let start = u64::min(a, b);
                                let end = u64::max(a, b);
                                let scan = db
                                    .scan(start..=end)
                                    .unwrap()
                                    .collect::<Result<Vec<_>, _>>()
                                    .unwrap();
                                let mut oracle_scan: Vec<_> = oracle
                                    .iter()
                                    .filter_map(|(&key, &value)| {
                                        (start..=end).contains(&key).then_some((key, value))
                                    })
                                    .collect();
                                oracle_scan.sort_unstable();
                                assert_eq!(scan, oracle_scan);
                                format!("scan {start}..={end} ==> # = {}", scan.len())
                            }
                            _ => unreachable!(),
                        };

                        if j % 256 == 0 {
                            println!("{i}::{j}. {command_description}");
                        }
                    }
                });
            }
        });

        Ok(())
    }
}
