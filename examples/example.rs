use std::fs;

use bearr::{Database, DbConfiguration, DbError, LsmConfiguration};

/// Formats the result of a database scan.
fn format_db_scan(
    scan: impl Iterator<Item = Result<(u64, u64), DbError>>,
) -> Result<String, DbError> {
    let contents: Result<Vec<(u64, u64)>, DbError> = scan.collect();
    let contents: Vec<String> = contents?
        .iter()
        .map(|(key, value)| format!("{key} => {value}"))
        .collect();
    Ok(format!("{{{}}}", contents.join(", ")))
}

/// Prints all key-value pairs currently in the database.
fn print_db_contents(db: &Database) -> Result<(), DbError> {
    let scan = db.scan(u64::MIN..=u64::MAX)?;
    println!("DB contains: {}", format_db_scan(scan)?);
    Ok(())
}

fn main() -> Result<(), DbError> {
    // `Database::create` returns an error if the path already exists,
    // so we need to delete any previous instance of the database.
    _ = fs::remove_dir_all("bearr_example");

    // Create a new database at the path `./bearr_example`.
    // See the documentation for more details on the configuration options.
    println!("Creating database");
    let mut db = Database::create(
        "bearr_example",
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
    println!();

    // Insert key-value pairs, inspecting the database along the way.
    for (key, value) in [
        (3, 14),
        (1, 59),
        (2, 65),
        (3, 58),
        (9, 79),
        (3, 23),
        (8, 46),
        (2, 64),
        (3, 38),
    ] {
        println!("Inserting {key} => {value}");
        db.put(key, value)?;
        print_db_contents(&db)?;
        println!();
    }

    // Get the values corresponding to keys 0-9.
    for key in 0..10 {
        println!("Get {key}: {:?}", db.get(key)?);
    }
    println!();

    // Close the database.
    // Automatically flushes the database, ignoring any errors.
    println!("Closing database");
    drop(db);
    println!();

    // Reopen the database.
    println!("Opening database");
    let mut db = Database::open("bearr_example")?;
    print_db_contents(&db)?;
    println!();

    // Get all key-value pairs in a range.
    let scan = db.scan(2..=8)?;
    println!("Scan 2..=8: {}", format_db_scan(scan)?);
    println!();

    // Delete key-value pairs with odd-number keys 1-9.
    for key in [1, 3, 5, 7, 9] {
        println!("Deleting {key}");
        db.delete(key)?;
        print_db_contents(&db)?;
        println!();
    }

    // Manually flush the database.
    // Call this explicitly if you want to handle errors that arise when flushing.
    println!("Manually flushing database");
    db.flush()?;
    println!();

    // Implicitly close the database by dropping it as it goes out of scope.
    println!("Closing database");
    Ok(())
}
