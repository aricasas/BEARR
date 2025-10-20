use bearr::Database;

const M: usize = 1048576;

fn main() {
    let config = bearr::DbConfiguration {
        memtable_size: 2 * M,
    };
    let mut db = Database::create("poop_db", config).unwrap();

    for i in 0..(100 * M as u64) {
        let k = i;
        let v = k * 10;
        db.put(k, v).unwrap();
        if i % M as u64 == 0 {
            eprintln!("Put {i}");
        }
    }

    let db = Database::open("poop_db").unwrap();
    let full_scan = db.scan(0..=u64::MAX).unwrap();

    eprintln!("Scan completed, n_entries={}", full_scan.len());

    for (k, v) in full_scan {
        assert_eq!(k * 10, v);
    }
}
