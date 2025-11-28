use std::{
    ops::RangeBounds,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

use bearr::{Database, DbConfiguration, LsmConfiguration};
use serde::{Deserialize, Serialize};

fn main() {
    let db_config = DbConfiguration {
        buffer_pool_capacity: 65_536, // 65,536 pages = 256 Mib
        write_buffering: 96,
        readahead_buffering: 128,
        lsm_configuration: LsmConfiguration {
            size_ratio: 4,
            memtable_capacity: 655_360, // 655,360 rows = 10 MiB
            bloom_filter_bits: 11,
        },
    };

    let total_entries = 64 * 1024 * 1024; // 64M rows = 1 GiB
    let sample_spacing = 1024 * 1024; // Sample every 1M rows inserted = every 16 MiB

    bench_put(BenchPutConfig {
        out_path: "bench_put.csv",
        total_entries,
        key_range: ..,
        sample_spacing,
        db_config,
    });

    let mut keys: Vec<u64> = vec![0; total_entries];
    let buffer: &mut [u8] = bytemuck::cast_slice_mut(&mut keys);
    fastrand::fill(buffer);

    bench_get(BenchGetConfig {
        out_path: if cfg!(feature = "binary_search") {
            "bench_get_binary.csv"
        } else {
            "bench_get.csv"
        },
        total_entries,
        key_list: &keys,
        percentage_from_key_list: 1.0,
        get_key_range: ..,
        sample_spacing,
        gets_per_sample: 1000,
        db_config,
    });

    for num_threads in [4, 16, 64, 256] {
        bench_concurrent_get(BenchConcurrentGetConfig {
            out_path: format!("bench_concurrent_get_{}.csv", num_threads),
            total_entries,
            num_threads,
            key_list: &keys,
            percentage_from_key_list: 1.0,
            get_key_range: ..,
            sample_spacing,
            gets_per_sample_per_thread: if num_threads == 256 { 500 } else { 1000 },
            db_config,
        });
    }

    bench_scan(BenchScanConfig {
        out_path: "bench_scan.csv",
        total_entries,
        key_range: ..,
        scan_start_key_range: ..,
        scans_per_sample: 300,
        entries_per_scan: 1000,
        sample_spacing,
        db_config,
    });

    for num_threads in [4, 16, 64] {
        bench_concurrent_scan(BenchConcurrentScanConfig {
            out_path: format!("bench_concurrent_scan_{}.csv", num_threads),
            total_entries,
            num_threads,
            key_range: ..,
            scan_start_key_range: ..,
            scans_per_sample_per_thread: 300,
            entries_per_scan: 1000,
            sample_spacing,
            db_config,
        });
    }

    bench_full_scan(BenchFullScanConfig {
        out_path: "bench_full_scan.csv",
        total_entries,
        key_range: ..,
        sample_spacing,
        db_config,
    });
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchPutSample {
    elapsed_time: f64,
    n_entries: usize,
    puts_time: f64,
    throughput_per_sec: f64,
}

struct BenchPutConfig<P: AsRef<Path>, R: RangeBounds<u64> + Clone> {
    out_path: P,
    total_entries: usize,
    key_range: R,
    sample_spacing: usize,
    db_config: DbConfiguration,
}

fn bench_put<P: AsRef<Path>, R: RangeBounds<u64> + Clone>(bench_config: BenchPutConfig<P, R>) {
    let BenchPutConfig {
        out_path,
        total_entries,
        key_range,
        sample_spacing,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_put_db");

    eprintln!("Running put benchmark: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_put_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    let start = Instant::now();
    let mut puts_duration = Duration::ZERO;

    for n_entries in 1..=total_entries {
        let key = rng.u64(key_range.clone());
        let val = rng.u64(..);

        let now = Instant::now();
        db.put(key, val).unwrap();
        puts_duration += now.elapsed();

        if n_entries % sample_spacing == 0 {
            let now = Instant::now();
            let elapsed_time = (now - start).as_secs_f64();
            let puts_time = puts_duration.as_secs_f64();
            let throughput_per_sec = sample_spacing as f64 / puts_time;

            data.push(BenchPutSample {
                elapsed_time,
                n_entries,
                puts_time,
                throughput_per_sec,
            });

            puts_duration = Duration::ZERO;
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data.iter() {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_put_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished put benchmark: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchGetSample {
    n_entries: usize,
    gets_time: f64,
    throughput_per_sec: f64,
    percentage_successful_gets: f64,
}

struct BenchGetConfig<'a, P: AsRef<Path>, R: RangeBounds<u64> + Clone> {
    out_path: P,
    total_entries: usize,
    key_list: &'a Vec<u64>,
    percentage_from_key_list: f32,
    get_key_range: R,
    sample_spacing: usize,
    gets_per_sample: usize,
    db_config: DbConfiguration,
}

fn bench_get<P: AsRef<Path>, R: RangeBounds<u64> + Clone>(bench_config: BenchGetConfig<P, R>) {
    let BenchGetConfig {
        out_path,
        total_entries,
        key_list,
        percentage_from_key_list,
        get_key_range,
        sample_spacing,
        gets_per_sample,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_get_db");

    eprintln!("Running get benchmark: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_get_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    for n_entries in 1..=total_entries {
        let key = key_list[n_entries - 1];
        let val = rng.u64(..);

        db.put(key, val).unwrap();

        if n_entries % sample_spacing == 0 {
            let mut num_successful_gets = 0;

            let mut gets_time = Duration::ZERO;

            for i in 0..gets_per_sample {
                let get_from_key_list =
                    (i as f32 / gets_per_sample as f32) < percentage_from_key_list;

                let key = if get_from_key_list {
                    key_list[fastrand::usize(0..n_entries)]
                } else {
                    rng.u64(get_key_range.clone())
                };

                let now = Instant::now();
                let val = db.get(key).unwrap();
                gets_time += now.elapsed();

                if val.is_some() {
                    num_successful_gets += 1;
                }
            }

            let gets_time = gets_time.as_secs_f64();
            let throughput_per_sec = gets_per_sample as f64 / gets_time;
            let percentage_successful_gets = num_successful_gets as f64 / gets_per_sample as f64;
            data.push(BenchGetSample {
                n_entries,
                gets_time,
                throughput_per_sec,
                percentage_successful_gets,
            });
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_get_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished get benchmark: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}

struct BenchConcurrentGetConfig<'a, P: AsRef<Path>, R: RangeBounds<u64> + Clone + Send + Sync> {
    out_path: P,
    total_entries: usize,
    num_threads: usize,
    key_list: &'a Vec<u64>,
    percentage_from_key_list: f32,
    get_key_range: R,
    sample_spacing: usize,
    gets_per_sample_per_thread: usize,
    db_config: DbConfiguration,
}
fn bench_concurrent_get<P: AsRef<Path>, R: RangeBounds<u64> + Clone + Send + Sync>(
    bench_config: BenchConcurrentGetConfig<P, R>,
) {
    let BenchConcurrentGetConfig {
        out_path,
        total_entries,
        num_threads,
        key_list,
        percentage_from_key_list,
        get_key_range,
        sample_spacing,
        gets_per_sample_per_thread,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_concurrent_get_db");

    eprintln!("Running concurrent get benchmark with {num_threads} threads: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_concurrent_get_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    for n_entries in 1..=total_entries {
        let key = key_list[n_entries - 1];
        let val = rng.u64(..);

        db.put(key, val).unwrap();

        if n_entries % sample_spacing == 0 {
            let num_successful_gets = AtomicUsize::new(0);
            let now = Instant::now();

            thread::scope(|scope| {
                for _ in 0..num_threads {
                    scope.spawn(|| {
                        let mut rng = fastrand::Rng::new();

                        let mut thread_successful_gets = 0;

                        for i in 0..gets_per_sample_per_thread {
                            let get_from_key_list = (i as f32 / gets_per_sample_per_thread as f32)
                                < percentage_from_key_list;

                            let key = if get_from_key_list {
                                key_list[rng.usize(0..n_entries)]
                            } else {
                                rng.u64(get_key_range.clone())
                            };

                            let val = db.get(key).unwrap();

                            if val.is_some() {
                                thread_successful_gets += 1;
                            }
                        }

                        num_successful_gets.fetch_add(thread_successful_gets, Ordering::Relaxed);
                    });
                }
            });
            let gets_time = now.elapsed().as_secs_f64();

            let throughput_per_sec = (num_threads * gets_per_sample_per_thread) as f64 / gets_time;
            let percentage_successful_gets = num_successful_gets.into_inner() as f64
                / (num_threads * gets_per_sample_per_thread) as f64;

            data.push(BenchGetSample {
                n_entries,
                gets_time,
                throughput_per_sec,
                percentage_successful_gets,
            });
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_concurrent_get_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished concurrent get benchmark with {num_threads} threads: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchScanSample {
    n_entries: usize,
    n_scanned_rows: usize,
    scans_time: f64,
    throughput_per_sec: f64,
}

struct BenchScanConfig<P: AsRef<Path>, R1: RangeBounds<u64> + Clone, R2: RangeBounds<u64> + Clone> {
    out_path: P,
    total_entries: usize,
    key_range: R1,
    scan_start_key_range: R2,
    scans_per_sample: usize,
    entries_per_scan: usize,
    sample_spacing: usize,
    db_config: DbConfiguration,
}

fn bench_scan<P: AsRef<Path>, R1: RangeBounds<u64> + Clone, R2: RangeBounds<u64> + Clone>(
    bench_config: BenchScanConfig<P, R1, R2>,
) {
    let BenchScanConfig {
        out_path,
        total_entries,
        key_range,
        scan_start_key_range,
        scans_per_sample,
        entries_per_scan,
        sample_spacing,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_scan_db");

    eprintln!("Running scan benchmark: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_scan_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    for n_entries in 1..=total_entries {
        let key = rng.u64(key_range.clone());
        let val = rng.u64(..);

        db.put(key, val).unwrap();

        if n_entries % sample_spacing == 0 {
            let now = Instant::now();

            let mut n_scanned_rows = 0;

            for _ in 0..scans_per_sample {
                let scan_start = rng.u64(scan_start_key_range.clone());

                let scan = db
                    .scan(scan_start..=u64::MAX)
                    .unwrap()
                    .take(entries_per_scan);
                n_scanned_rows += scan.inspect(|row| assert!(row.is_ok())).count();
            }

            let scan_time = now.elapsed().as_secs_f64();
            let throughput_per_sec = n_scanned_rows as f64 / scan_time;

            data.push(BenchScanSample {
                n_entries,
                n_scanned_rows,
                scans_time: scan_time,
                throughput_per_sec,
            });
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_scan_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished scan benchmark: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}

struct BenchConcurrentScanConfig<
    P: AsRef<Path>,
    R1: RangeBounds<u64> + Clone,
    R2: RangeBounds<u64> + Clone,
> {
    out_path: P,
    total_entries: usize,
    num_threads: usize,
    key_range: R1,
    scan_start_key_range: R2,
    scans_per_sample_per_thread: usize,
    entries_per_scan: usize,
    sample_spacing: usize,
    db_config: DbConfiguration,
}

fn bench_concurrent_scan<
    P: AsRef<Path>,
    R1: RangeBounds<u64> + Clone + Send + Sync,
    R2: RangeBounds<u64> + Clone + Send + Sync,
>(
    bench_config: BenchConcurrentScanConfig<P, R1, R2>,
) {
    let BenchConcurrentScanConfig {
        out_path,
        total_entries,
        num_threads,
        key_range,
        scan_start_key_range,
        scans_per_sample_per_thread,
        entries_per_scan,
        sample_spacing,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_concurrent_scan_db");

    eprintln!("Running concurrent scan benchmark with {num_threads} threads: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_concurrent_scan_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    for n_entries in 1..=total_entries {
        let key = rng.u64(key_range.clone());
        let val = rng.u64(..);

        db.put(key, val).unwrap();

        if n_entries % sample_spacing == 0 {
            let n_scanned_rows = AtomicUsize::new(0);

            let now = Instant::now();
            thread::scope(|scope| {
                for _ in 0..num_threads {
                    scope.spawn(|| {
                        let mut rng = fastrand::Rng::new();

                        let mut thread_n_scanned_rows = 0;

                        for _ in 0..scans_per_sample_per_thread {
                            let scan_start = rng.u64(scan_start_key_range.clone());

                            let scan = db
                                .scan(scan_start..=u64::MAX)
                                .unwrap()
                                .take(entries_per_scan);
                            thread_n_scanned_rows +=
                                scan.inspect(|row| assert!(row.is_ok())).count();
                        }

                        n_scanned_rows.fetch_add(thread_n_scanned_rows, Ordering::Relaxed);
                    });
                }
            });
            let scans_time = now.elapsed().as_secs_f64();
            let n_scanned_rows = n_scanned_rows.load(Ordering::Relaxed);

            let throughput_per_sec = n_scanned_rows as f64 / scans_time;

            data.push(BenchScanSample {
                n_entries,
                n_scanned_rows,
                scans_time,
                throughput_per_sec,
            });
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_concurrent_scan_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished scan benchmark: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}

struct BenchFullScanConfig<P: AsRef<Path>, R: RangeBounds<u64> + Clone> {
    out_path: P,
    total_entries: usize,
    key_range: R,
    sample_spacing: usize,
    db_config: DbConfiguration,
}

fn bench_full_scan<P: AsRef<Path>, R: RangeBounds<u64> + Clone>(
    bench_config: BenchFullScanConfig<P, R>,
) {
    let BenchFullScanConfig {
        out_path,
        total_entries,
        key_range,
        sample_spacing,
        db_config,
    } = bench_config;

    let _ = std::fs::remove_dir_all("bench_full_scan_db");

    eprintln!("Running full scan benchmark: N={total_entries}");
    let bench_start = Instant::now();

    let mut db = Database::create("bench_full_scan_db", db_config).unwrap();
    let mut rng = fastrand::Rng::new();

    let mut data = Vec::with_capacity(total_entries.div_ceil(sample_spacing));

    for n_entries in 1..=total_entries {
        let key = rng.u64(key_range.clone());
        let val = rng.u64(..);

        db.put(key, val).unwrap();

        if n_entries % sample_spacing == 0 {
            let now = Instant::now();

            let full_scan = db.scan(0..=u64::MAX).unwrap();
            let n_scanned_rows = full_scan.inspect(|row| assert!(row.is_ok())).count();

            let scan_time = now.elapsed().as_secs_f64();
            let throughput_per_sec = n_scanned_rows as f64 / scan_time;

            data.push(BenchScanSample {
                n_entries,
                n_scanned_rows,
                scans_time: scan_time,
                throughput_per_sec,
            });
        }
    }

    drop(db);

    let mut csv_writer = csv::Writer::from_path(out_path).unwrap();
    for record in data {
        csv_writer.serialize(record).unwrap();
    }
    csv_writer.flush().unwrap();

    if !cfg!(feature = "keep_test_files") {
        std::fs::remove_dir_all("bench_full_scan_db").unwrap();
    }

    let bench_elapsed = bench_start.elapsed();
    eprintln!(
        "Finished full scan benchmark: time={:.3} secs",
        bench_elapsed.as_secs_f64()
    );
}
