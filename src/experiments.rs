use bearr::Database;

const M: usize = 1024 * 1024;

fn main() {
    let config = bearr::DbConfiguration {
        memtable_capacity: 2 * M,
        buffer_pool_capacity: 16 * 1024,
        write_buffering: 16,
    };
    let mut db = Database::create("poop_db", config).unwrap();
    let mut rng = fastrand::Rng::new();

    for i in 0..(100 * M as u64) {
        let k = rng.u64(0..(100 * M as u64));
        let v = k * 10;
        db.put(k, v).unwrap();
        if i % M as u64 == 0 {
            eprintln!("Put {i}");
        }
    }
    drop(db);

    let db = Database::open("poop_db").unwrap();
    let full_scan = db.scan(0..=u64::MAX).unwrap();

    let mut n_entries = 0;

    for res in full_scan {
        let (k, v) = res.unwrap();
        assert_eq!(k * 10, v);
        n_entries += 1;
        if n_entries % M == 0 {
            eprintln!("Scanned {n_entries}");
        }
    }
    eprintln!("Scan completed, n_entries={n_entries}");

    let full_scan = db.scan(0..=u64::MAX).unwrap();

    let mut n_entries = 0;

    for res in full_scan {
        let (k, v) = res.unwrap();
        assert_eq!(k * 10, v);
        n_entries += 1;
        if n_entries % M == 0 {
            eprintln!("Scanned {n_entries}");
        }
    }
    eprintln!("Scan completed, n_entries={n_entries}");

    drop(db);
}
