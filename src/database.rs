use std::{
    fs::{self, File},
    ops::RangeInclusive,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    DbError,
    memtable::MemTable,
    merge::{self, MergedIterator},
    sst::Sst,
};

#[cfg(not(feature = "mock"))]
use crate::file_system::FileSystem;

#[cfg(feature = "mock")]
use crate::mock::FileSystem;

/// An open connection to a database
pub struct Database {
    configuration: DbConfiguration,
    name: PathBuf,
    memtable: MemTable<u64, u64>,
    ssts: Vec<Sst>,
    file_system: FileSystem,
}

/// Configuration options for a database
#[derive(Serialize, Deserialize)]
pub struct DbConfiguration {
    pub memtable_capacity: usize,
    pub buffer_pool_capacity: usize,
    pub write_buffering: usize,
}

impl DbConfiguration {
    fn validate(&self) -> Result<(), DbError> {
        if self.memtable_capacity > 0 && self.buffer_pool_capacity >= 16 && self.write_buffering > 0
        {
            Ok(())
        } else {
            Err(DbError::InvalidConfiguration)
        }
    }
}

/// Metadata for a database
#[derive(Serialize, Deserialize)]
struct DbMetadata {
    num_ssts: usize,
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

        let metadata = DbMetadata { num_ssts: 0 };
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

        let memtable = MemTable::new(configuration.memtable_capacity)?;
        let file_system = FileSystem::new(
            configuration.buffer_pool_capacity,
            configuration.write_buffering,
        )?;

        let mut ssts = Vec::with_capacity(metadata.num_ssts);
        for i in 0..metadata.num_ssts {
            let sst = Sst::open(name.join(i.to_string()), &file_system)?;
            ssts.push(sst);
        }

        Ok(Self {
            configuration,
            name: name.to_path_buf(),
            memtable,
            ssts,
            file_system,
        })
    }

    /// Inserts the given key-value pair into the database,
    /// flushing the memtable if it reaches capacity.
    ///
    /// Returns an error if flushing fails. See `Database::flush` for more info.
    pub fn put(&mut self, key: u64, value: u64) -> Result<(), DbError> {
        self.memtable.put(key, value);

        // Ensure memtable remains below capacity for the next put
        if self.memtable.size() == self.configuration.memtable_capacity {
            self.flush()?;
        }

        Ok(())
    }

    fn num_ssts(&self) -> usize {
        self.ssts.len()
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
        if self.memtable.size() == 0 {
            return Ok(());
        }

        let key_values = self.memtable.scan(u64::MIN..=u64::MAX)?;

        let path = self.name.join(self.num_ssts().to_string());
        let sst = Sst::create(key_values.map(Ok), &path, &mut self.file_system)?;

        let metadata = DbMetadata {
            num_ssts: self.num_ssts() + 1,
        };
        let metadata_file = File::create(self.name.join(METADATA_FILENAME))?;
        serde_json::to_writer_pretty(metadata_file, &metadata)?;

        self.ssts.push(sst);
        self.memtable.clear();

        Ok(())
    }

    /// Returns the value associated with the given key, if it exists.
    ///
    /// Returns an error if searching fails in an SST.
    pub fn get(&self, key: u64) -> Result<Option<u64>, DbError> {
        let val = self.memtable.get(key);
        if val.is_some() {
            return Ok(val);
        }

        // Further-back (higher-numbered) SSTs are newer, so search them first.
        for sst in self.ssts.iter().rev() {
            let val = sst.get(key, &self.file_system)?;
            if val.is_some() {
                return Ok(val);
            }
        }

        Ok(None)
    }

    /// Returns a sorted list of all key-value pairs where the key is in the given range.
    ///
    /// Returns an error if scanning fails in the memtable or SSTs.
    pub fn scan(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64), DbError>>, DbError> {
        let mut scans = Vec::new();

        let memtable_scan = self.memtable.scan(range.clone())?;
        scans.push(merge::Sources::MemTable(memtable_scan));

        for sst in self.ssts.iter().rev() {
            let sst_scan = sst.scan(range.clone(), &self.file_system)?;
            scans.push(merge::Sources::BTree(sst_scan));
        }

        MergedIterator::new(scans)
    }

    /// Closes the database.
    ///
    /// If flushing fails, returns an error along with `self` to allow retrying the close.
    pub fn close(mut self) -> Result<(), (Self, DbError)> {
        self.flush().map_err(|e| (self, e))
    }
}

/// The database is flushed upon dropping.
///
/// Errors are ignored. To handle them, call `Database::flush` or `Database::close` manually.
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
                memtable_capacity: 3,
                buffer_pool_capacity: 16,
                write_buffering: 1,
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
                    memtable_capacity: 10,
                    buffer_pool_capacity: 16,
                    write_buffering: 1,
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
            db.close().map_err(|(_, e)| e)?;
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
