use std::{
    sync::{Arc, RwLock},
    thread,
    time::Instant,
};

use bearr::Database;

const M: usize = 1024 * 1024;

fn main() {
    _ = std::fs::remove_dir_all("poop_db");

    let config = bearr::DbConfiguration {
        memtable_capacity: 2 * M,
        buffer_pool_capacity: 16 * 1024,
        write_buffering: 32,
    };
    let mut db = Database::create("poop_db", config).unwrap();
    let mut rng = fastrand::Rng::new();

    let num_puts = 10 * M;

    let now = Instant::now();
    for i in 0..num_puts {
        let k = rng.u64(0..(100 * M as u64));
        let v = k * 10;
        db.put(k, v).unwrap();
        if i % M == 0 {
            println!("Put {i}");
        }
    }

    let elapsed = now.elapsed();

    println!("{num_puts} puts completed, took {}ms", elapsed.as_millis());
    println!("Avg put took {}s", elapsed.as_secs_f64() / num_puts as f64);

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
            println!("Single thread Scanned {n_entries}");
        }
    }
    let elapsed = now.elapsed();

    println!(
        "Single thread Scan completed, n_entries={n_entries}, took {}ms",
        elapsed.as_millis()
    );
    println!(
        "Single thread Avg per entry took {}s",
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

    println!("{num_gets} gets completed, took {}ms", elapsed.as_millis());
    println!("Avg get took {}s", elapsed.as_secs_f64() / num_gets as f64);

    drop(db);

    let db = Database::open("poop_db").unwrap();
    let db = Arc::new(RwLock::new(db));

    let db_1 = db.clone();
    let db_2 = db.clone();
    let thread_1 = thread::spawn(move || {
        let db = db_1;
        let db = db.read().unwrap();

        let full_scan = db.scan(0..=u64::MAX).unwrap();

        let mut n_entries = 0;
        let now = Instant::now();

        for res in full_scan {
            let (k, v) = res.unwrap();
            assert_eq!(k * 10, v);
            n_entries += 1;
            if n_entries % M == 0 {
                println!("Thread 1. Scanned {n_entries}");
            }
        }
        let elapsed = now.elapsed();

        println!(
            "Thread 1. Scan completed, n_entries={n_entries}, took {}ms",
            elapsed.as_millis()
        );
        println!(
            "Thread 1. Avg per entry took {}s",
            elapsed.as_secs_f64() / n_entries as f64
        );
    });
    let thread_2 = thread::spawn(move || {
        let db = db_2;
        let db = db.read().unwrap();

        let full_scan = db.scan(0..=u64::MAX).unwrap();

        let mut n_entries = 0;
        let now = Instant::now();

        for res in full_scan {
            let (k, v) = res.unwrap();
            assert_eq!(k * 10, v);
            n_entries += 1;
            if n_entries % M == 0 {
                println!("Thread 2. Scanned {n_entries}");
            }
        }
        let elapsed = now.elapsed();

        println!(
            "Thread 2. Scan completed, n_entries={n_entries}, took {}ms",
            elapsed.as_millis()
        );
        println!(
            "Thread 2. Avg per entry took {}s",
            elapsed.as_secs_f64() / n_entries as f64
        );
    });

    thread_1.join().unwrap();
    thread_2.join().unwrap();
}
