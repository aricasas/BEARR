use std::time::Instant;

use bearr::Database;

const M: usize = 1024 * 1024;

fn main() {
    let config = bearr::DbConfiguration {
        memtable_capacity: 2 * M,
        buffer_pool_capacity: 16 * 1024,
        write_buffering: 32,
    };
    let mut db = Database::create("poop_db", config).unwrap();
    let mut rng = fastrand::Rng::new();

    let num_puts = 100 * M;

    let now = Instant::now();
    for i in 0..num_puts {
        let k = rng.u64(0..(100 * M as u64));
        let v = k * 10;
        db.put(k, v).unwrap();
        if i % M == 0 {
            eprintln!("Put {i}");
        }
    }

    let elapsed = now.elapsed();

    eprintln!("{num_puts} puts completed, took {}ms", elapsed.as_millis());
    eprintln!("Avg get took {}s", elapsed.as_secs_f64() / num_puts as f64);

    drop(db);

    let db = Database::open("poop_db").unwrap();
    let full_scan = db.scan(0..=u64::MAX).unwrap();

    let mut n_entries = 0;
    let now = Instant::now();

    for res in full_scan {
        let (k, v) = res.unwrap();
        assert_eq!(k * 10, v);
        n_entries += 1;
        if n_entries % M == 0 {
            eprintln!("Scanned {n_entries}");
        }
    }
    let elapsed = now.elapsed();

    eprintln!(
        "Scan completed, n_entries={n_entries}, took {}ms",
        elapsed.as_millis()
    );
    eprintln!(
        "Avg per entry took {}s",
        elapsed.as_secs_f64() / n_entries as f64
    );

    drop(db);

    let db = Database::open("poop_db").unwrap();
    let num_gets = 2000;

    let now = Instant::now();
    for _ in 0..num_gets {
        let k = rng.u64(0..(100 * M as u64));
        let expected_v_if_exists = 10 * k;

        assert!(
            db.get(k)
                .unwrap()
                .is_none_or(|val| val == expected_v_if_exists)
        );
    }
    let elapsed = now.elapsed();

    eprintln!("{num_gets} gets completed, took {}ms", elapsed.as_millis());
    eprintln!("Avg get took {}s", elapsed.as_secs_f64() / num_gets as f64);

    drop(db);
}
