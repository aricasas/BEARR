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
    wal_buffer: Vec<(u64, u64)>,
    wal_file: File,
    wal_enabled: bool,
}

/// Configuration options for a database.
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
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
    /// Number of operations to buffer before flushing to WAL
    pub wal_buffer_size: Option<usize>,
}

impl DbConfiguration {
    fn validate(&self) -> Result<(), DbError> {
        self.lsm_configuration.validate()?;
        if self.buffer_pool_capacity >= 16
            && self.write_buffering > 0
            && self.readahead_buffering > 0
            && self.wal_buffer_size.is_none_or(|x| x > 0)
        {
            Ok(())
        } else {
            Err(DbError::InvalidConfiguration)
        }
    }
}

/// Metadata for a database
#[derive(Serialize, Deserialize, Debug)]
struct DbMetadata {
    lsm_metadata: LsmMetadata,
}

const CONFIG_FILENAME: &str = "config.json";
const METADATA_FILENAME: &str = "metadata.json";
const LOG_FILENAME: &str = "WAL.log";

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

        File::create_new(name.join(LOG_FILENAME))?;

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
        let mut db = Self::new(name, configuration, metadata)?;
        db.wal_enabled = configuration.wal_buffer_size.is_some();

        if db.wal_enabled {
            // Replay WAL
            db.replay_wal()?;
        }
        Ok(db)
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

        let wal_file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(name.join(LOG_FILENAME))?;

        Ok(Self {
            name: name.to_path_buf(),
            lsm,
            file_system,
            wal_buffer: Vec::with_capacity(configuration.wal_buffer_size.unwrap_or(0)),
            wal_file,
            wal_enabled: configuration.wal_buffer_size.is_some(),
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

        if self.wal_enabled {
            // Add to WAL buffer
            self.wal_buffer.push((key, value));

            // Flush WAL buffer if it's full
            if self.wal_buffer.len() >= self.wal_buffer.capacity() {
                self.flush_wal_buffer()?;
            }
        }

        let sst_flushed = self.lsm.put(key, value, &self.file_system)?;

        if sst_flushed {
            self.flush()?;
        }

        Ok(())
    }

    /// Removes the key-value pair with given key from the database, if one exists.
    ///
    /// Has no effect on the set of key-value pairs in the database
    /// if the pair with the given key does not exist,
    /// but may affect how the data is internally stored.
    ///
    /// Returns an error if deletion fails.
    pub fn delete(&mut self, key: u64) -> Result<(), DbError> {
        if self.wal_enabled {
            // Add to WAL buffer with TOMBSTONE
            self.wal_buffer.push((key, TOMBSTONE));
            // Flush WAL buffer if it's full
            if self.wal_buffer.len() >= self.wal_buffer.capacity() {
                self.flush_wal_buffer()?;
            }
        }
        let sst_flushed = self.lsm.delete(key, &self.file_system)?;

        if sst_flushed {
            self.flush()?;
        }

        Ok(())
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
        // Flush any pending WAL entries
        if self.wal_enabled {
            self.flush_wal_buffer()?;
        }

        self.lsm.flush_memtable(&self.file_system)?;

        let lsm_metadata = self.lsm.metadata();
        let metadata = DbMetadata { lsm_metadata };
        let metadata_file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(self.name.join(METADATA_FILENAME))?;
        serde_json::to_writer_pretty(&metadata_file, &metadata)?;
        metadata_file.sync_all()?;
        // Checkpoint WAL after successful memtable flush
        if self.wal_enabled {
            self.checkpoint_wal()?;
        }
        Ok(())
    }

    fn flush_wal_buffer(&mut self) -> Result<(), DbError> {
        assert!(self.wal_enabled);
        use std::io::Write;

        if self.wal_buffer.is_empty() {
            return Ok(());
        }

        for &(key, value) in &self.wal_buffer {
            writeln!(&mut self.wal_file, "{},{}", key, value)?;
        }
        self.wal_file.flush()?;
        self.wal_file.sync_all()?;
        self.wal_buffer.clear();

        Ok(())
    }

    /// Replays WAL entries into memtable
    fn replay_wal(&mut self) -> Result<(), DbError> {
        assert!(self.wal_enabled);
        use std::io::{BufRead, BufReader};

        let wal_path = self.name.join(LOG_FILENAME);
        let file = File::open(&wal_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 2 {
                let key: u64 = parts[0]
                    .parse()
                    .map_err(|_| DbError::IoError("wrong Key".to_string()))?;
                let value: u64 = parts[1]
                    .parse()
                    .map_err(|_| DbError::IoError("wrong Val".to_string()))?;

                // Replay without WAL buffering to avoid infinite recursion
                self.lsm.put(key, value, &self.file_system)?;
            }
        }

        Ok(())
    }

    /// Checkpoints WAL by truncating it
    fn checkpoint_wal(&mut self) -> Result<(), DbError> {
        assert!(self.wal_enabled);
        // Flush any pending entries first
        self.flush_wal_buffer()?;

        // Truncate WAL file
        self.wal_file = File::create(self.name.join(LOG_FILENAME))?;

        Ok(())
    }
}

/// The database is flushed upon dropping.
///
/// Errors are ignored. To handle them, call `Database::flush` manually.
impl Drop for Database {
    fn drop(&mut self) {
        if self.wal_enabled {
            _ = self.flush_wal_buffer();
        }
        _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ops::Range, thread};

    use anyhow::Result;

    use crate::test_util::{TestPath, get_path};

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
                wal_buffer_size: Some(10),
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
                    wal_buffer_size: Some(10),
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

    #[allow(clippy::too_many_arguments)]
    fn create_db(
        name: &str,
        size_ratio: usize,
        memtable_capacity: usize,
        bloom_filter_bits: usize,
        buffer_pool_capacity: usize,
        write_buffering: usize,
        readahead_buffering: usize,
        wal_buffer_size: usize,
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
                wal_buffer_size: Some(wal_buffer_size),
            },
        )
    }

    #[test]
    fn test_errors() -> Result<()> {
        let ok_config = DbConfiguration {
            lsm_configuration: LsmConfiguration {
                size_ratio: 2,
                memtable_capacity: 1,
                bloom_filter_bits: 0,
            },
            buffer_pool_capacity: 16,
            write_buffering: 1,
            readahead_buffering: 1,
            wal_buffer_size: Some(10),
        };

        let path = &test_path("errors");
        let mut db = Database::create(path, ok_config)?;
        // DB already exists
        assert!(matches!(
            Database::create(path, ok_config),
            Err(DbError::IoError(_))
        ));
        // Create with non-existent parent path
        assert!(matches!(
            Database::create(get_path("database", "monad"), ok_config),
            Err(DbError::IoError(_))
        ));
        // Open non-existent path
        assert!(matches!(
            Database::open(get_path("database", "monoid")),
            Err(DbError::IoError(_))
        ));

        assert_eq!(
            create_db("errors_bad_size_ratio", 1, 1, 0, 16, 1, 1, 10).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_memtable_capacity", 2, 0, 0, 16, 1, 1, 10).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_small_buffer_pool_capacity", 2, 1, 0, 15, 1, 1, 10).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_write_buffering", 2, 1, 0, 16, 0, 1, 10).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_no_readahead_buffering", 2, 1, 0, 16, 1, 0, 10).err(),
            Some(DbError::InvalidConfiguration)
        );
        assert_eq!(
            create_db("errors_zero_wal", 2, 1, 0, 16, 1, 1, 0).err(),
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
                wal_buffer_size: Some(64),
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
                Command::Get,
                Command::Put,
                Command::Delete,
                Command::Scan,
                Command::Get,
                Command::Put,
                Command::Delete,
                Command::Scan,
                Command::Get,
                Command::Put,
                Command::Delete,
                Command::Scan,
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
                wal_buffer_size: Some(16),
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
    mod wal_tests {
        use super::*;
        use crate::test_util::TestPath;
        use std::io::{BufRead, BufReader};

        fn test_path(name: &str) -> TestPath {
            TestPath::create("database_wal", name)
        }

        fn count_wal_entries(db_path: &Path) -> Result<usize> {
            let wal_path = db_path.join(LOG_FILENAME);
            let file = File::open(wal_path)?;
            let reader = BufReader::new(file);
            let count = reader
                .lines()
                .filter(|l| l.as_ref().map(|s| !s.is_empty()).unwrap_or(false))
                .count();
            Ok(count)
        }

        #[test]
        fn test_wal_buffering() -> Result<()> {
            let name = &test_path("wal_buffering");
            let mut db = Database::create(
                name,
                DbConfiguration {
                    buffer_pool_capacity: 16,
                    write_buffering: 1,
                    readahead_buffering: 1,
                    wal_buffer_size: Some(5), // Buffer 5 entries before flushing
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 2,
                        memtable_capacity: 100, // Large enough to not trigger memtable flush
                        bloom_filter_bits: 1,
                    },
                },
            )?;

            db.put(1, 10)?;
            db.put(2, 20)?;
            db.put(3, 30)?;

            // WAL file should still be empty (or very small if flushed)
            assert_eq!(db.wal_buffer.len(), 3);

            // Put 2 more entries - should trigger flush at 5
            db.put(4, 40)?;
            db.put(5, 50)?;

            // Buffer should be flushed and empty
            assert_eq!(db.wal_buffer.len(), 0);

            // WAL file should have 5 entries
            assert_eq!(count_wal_entries(name.as_ref())?, 5);

            Ok(())
        }

        #[test]
        fn test_wal_crash_recovery() -> Result<()> {
            let name = &test_path("wal_crash_recovery");

            // Phase 1: Create database and add entries
            {
                let mut db = Database::create(
                    name,
                    DbConfiguration {
                        buffer_pool_capacity: 16,
                        write_buffering: 1,
                        readahead_buffering: 1,
                        wal_buffer_size: Some(3),
                        lsm_configuration: LsmConfiguration {
                            size_ratio: 2,
                            memtable_capacity: 100,
                            bloom_filter_bits: 1,
                        },
                    },
                )?;

                // Add some entries that will be flushed to WAL
                db.put(1, 100)?;
                db.put(2, 200)?;
                db.put(3, 300)?;
                // Buffer flushed here (3 entries)

                // These values should not exist !!!
                db.put(4, 200)?;
                db.put(5, 300)?;
                // Simulate crash - drop without calling flush
                std::mem::forget(db);
            }

            // Phase 2: Reopen database and verify recovery
            {
                let db = Database::open(name)?;

                // All entries should be recovered
                assert_eq!(db.get(1)?, Some(100));
                assert_eq!(db.get(2)?, Some(200));
                assert_eq!(db.get(3)?, Some(300));
                assert_eq!(db.get(4)?, None);
                assert_eq!(db.get(5)?, None);
            }

            Ok(())
        }

        #[test]
        fn test_wal_checkpoint_on_flush() -> Result<()> {
            let name = &test_path("wal_checkpoint");
            let mut db = Database::create(
                name,
                DbConfiguration {
                    buffer_pool_capacity: 16,
                    write_buffering: 1,
                    readahead_buffering: 1,
                    wal_buffer_size: Some(2),
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 2,
                        memtable_capacity: 5,
                        bloom_filter_bits: 1,
                    },
                },
            )?;

            // Add entries to WAL
            db.put(1, 10)?;
            db.put(2, 20)?;
            // WAL buffer flushed

            db.put(3, 30)?;
            db.put(4, 40)?;
            // WAL buffer flushed again

            // WAL should have 4 entries
            assert_eq!(count_wal_entries(name.as_ref())?, 4);

            // Flush memtable - this should checkpoint (truncate) WAL
            db.flush()?;

            // WAL should be empty after checkpoint
            assert_eq!(count_wal_entries(name.as_ref())?, 0);

            // Data should still be accessible
            assert_eq!(db.get(1)?, Some(10));
            assert_eq!(db.get(2)?, Some(20));
            assert_eq!(db.get(3)?, Some(30));
            assert_eq!(db.get(4)?, Some(40));

            Ok(())
        }

        #[test]
        fn test_wal_with_deletes() -> Result<()> {
            let name = &test_path("wal_deletes");

            {
                let mut db = Database::create(
                    name,
                    DbConfiguration {
                        buffer_pool_capacity: 16,
                        write_buffering: 1,
                        readahead_buffering: 1,
                        wal_buffer_size: Some(3),
                        lsm_configuration: LsmConfiguration {
                            size_ratio: 2,
                            memtable_capacity: 100,
                            bloom_filter_bits: 1,
                        },
                    },
                )?;

                // Add and delete entries
                db.put(1, 100)?;
                db.put(2, 200)?;
                db.delete(2)?; // Delete key 2
                // Buffer flushed

                db.put(4, 400)?;
                // Buffer not flushed

                // Simulate crash
                std::mem::forget(db);
            }

            // Recover and verify
            {
                let db = Database::open(name)?;

                assert_eq!(db.get(1)?, Some(100));
                assert_eq!(db.get(2)?, None); // Should not show anything
                assert_eq!(db.get(4)?, None); // Doesnt Exist
            }

            Ok(())
        }

        #[test]
        fn test_wal_recovery_with_memtable_flush() -> Result<()> {
            let name = &test_path("wal_recovery_memtable_flush");

            {
                let mut db = Database::create(
                    name,
                    DbConfiguration {
                        buffer_pool_capacity: 16,
                        write_buffering: 1,
                        readahead_buffering: 1,
                        wal_buffer_size: Some(2),
                        lsm_configuration: LsmConfiguration {
                            size_ratio: 2,
                            memtable_capacity: 3,
                            bloom_filter_bits: 1,
                        },
                    },
                )?;

                // Add entries that will trigger memtable flush
                db.put(1, 10)?;
                db.put(2, 20)?;

                assert_eq!(count_wal_entries(name.as_ref())?, 2);
                println!("wal buffer contents {:?}", db.wal_buffer);
                // WAL flushed

                db.put(3, 30)?;

                assert_eq!(count_wal_entries(name.as_ref())?, 0);
                assert_eq!(db.get(1)?, Some(10));
                assert_eq!(db.get(2)?, Some(20));
                assert_eq!(db.get(3)?, Some(30));
                // Memtable flushed to SST, WAL checkpointed

                // Add more entries after checkpoint
                db.put(4, 40)?;
                db.put(5, 50)?;
                // WAL flushed

                db.put(6, 60)?;
                // In buffer, not yet in WAL

                // Simulate crash
                std::mem::forget(db);
            }

            // Recover
            {
                let db = Database::open(name)?;

                // All entries should be recovered
                assert_eq!(db.get(1)?, Some(10));
                assert_eq!(db.get(2)?, Some(20));
                assert_eq!(db.get(3)?, Some(30));
                assert_eq!(db.get(4)?, Some(40));
                assert_eq!(db.get(5)?, Some(50));
                assert_eq!(db.get(6)?, Some(60));
            }

            Ok(())
        }

        #[test]
        fn test_wal_scan_after_recovery() -> Result<()> {
            let name = &test_path("wal_scan_recovery");

            {
                let mut db = Database::create(
                    name,
                    DbConfiguration {
                        buffer_pool_capacity: 16,
                        write_buffering: 1,
                        readahead_buffering: 1,
                        wal_buffer_size: Some(3),
                        lsm_configuration: LsmConfiguration {
                            size_ratio: 2,
                            memtable_capacity: 100,
                            bloom_filter_bits: 1,
                        },
                    },
                )?;

                db.put(5, 50)?;
                db.put(1, 10)?;
                db.put(3, 30)?;
                // Buffer flushed

                db.put(2, 20)?;
                db.put(4, 40)?;
                // In buffer
                // Simulate crash
                std::mem::forget(db);
            }

            {
                let db = Database::open(name)?;

                // Scan should return sorted results
                let results = db.scan(1..=5)?.collect::<Result<Vec<_>, _>>()?;
                assert_eq!(results, vec![(1, 10), (3, 30), (5, 50)]);
            }

            Ok(())
        }

        #[test]
        fn test_wal_buffer_size_one() -> Result<()> {
            let name = &test_path("wal_buffer_one");
            let mut db = Database::create(
                name,
                DbConfiguration {
                    buffer_pool_capacity: 16,
                    write_buffering: 1,
                    readahead_buffering: 1,
                    wal_buffer_size: Some(1), // Flush every single operation
                    lsm_configuration: LsmConfiguration {
                        size_ratio: 2,
                        memtable_capacity: 100,
                        bloom_filter_bits: 1,
                    },
                },
            )?;

            db.put(1, 10)?;
            assert_eq!(count_wal_entries(name.as_ref())?, 1);

            db.put(2, 20)?;
            assert_eq!(count_wal_entries(name.as_ref())?, 2);

            db.delete(1)?;
            assert_eq!(count_wal_entries(name.as_ref())?, 3);

            Ok(())
        }
    }
}
