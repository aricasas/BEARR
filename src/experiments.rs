use bearr::Database;

const M: usize = 1048576;

fn main() {
    let config = bearr::DbConfiguration {
        memtable_capacity: 2 * M,
        buffer_pool_capacity: 1,
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

    let mut db = Database::open("poop_db").unwrap();
    let full_scan = db.scan(0..=u64::MAX).unwrap();

    eprintln!("Scan completed, n_entries={}", full_scan.len());

    for (k, v) in full_scan {
        assert_eq!(k * 10, v);
    }
}
