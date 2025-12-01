cargo build --release --bin experiments

./target/release/experiments --put bench_put_T2.csv --size-ratio 2
./target/release/experiments --put bench_put_T4.csv --size-ratio 4
./target/release/experiments --put bench_put_T8.csv --size-ratio 8

./target/release/experiments --get bench_get_T2.csv --ops-per-sample 1000 --size-ratio 2 --get-success-percentage 1.0
./target/release/experiments --get bench_get_T4.csv --ops-per-sample 1000 --size-ratio 4 --get-success-percentage 1.0
./target/release/experiments --get bench_get_T8.csv --ops-per-sample 1000 --size-ratio 8 --get-success-percentage 1.0

./target/release/experiments --concurrent-get bench_concurrent_get_1.csv --num-threads 1 --ops-per-sample 1000 --get-success-percentage 1.0
./target/release/experiments --concurrent-get bench_concurrent_get_4.csv --num-threads 4 --ops-per-sample 1000 --get-success-percentage 1.0
./target/release/experiments --concurrent-get bench_concurrent_get_16.csv --num-threads 16 --ops-per-sample 1000 --get-success-percentage 1.0
./target/release/experiments --concurrent-get bench_concurrent_get_64.csv --num-threads 64 --ops-per-sample 1000 --get-success-percentage 1.0
./target/release/experiments --concurrent-get bench_concurrent_get_256.csv --num-threads 256 --ops-per-sample 500 --get-success-percentage 1.0

./target/release/experiments --scan bench_scan.csv --ops-per-sample 300 --entries-per-scan 1000

./target/release/experiments --concurrent-scan bench_concurrent_scan_4.csv --num-threads 4 --ops-per-sample 300 --entries-per-scan 1000
./target/release/experiments --concurrent-scan bench_concurrent_scan_16.csv --num-threads 16 --ops-per-sample 300 --entries-per-scan 1000
./target/release/experiments --concurrent-scan bench_concurrent_scan_64.csv --num-threads 64 --ops-per-sample 300 --entries-per-scan 1000

./target/release/experiments --full-scan bench_full_scan.csv

./target/release/experiments --get bench_get_0pct.csv --ops-per-sample 1000 --get-success-percentage 0.0
./target/release/experiments --get bench_get_50pct.csv --ops-per-sample 1000 --get-success-percentage 0.5
./target/release/experiments --get bench_get_100pct.csv --ops-per-sample 1000 --get-success-percentage 1.0


cargo build --release --bin experiments --features binary_search
./target/release/experiments --get bench_get_binary_0pct.csv --ops-per-sample 1000 --get-success-percentage 0.0
./target/release/experiments --get bench_get_binary_50pct.csv --ops-per-sample 1000 --get-success-percentage 0.5
./target/release/experiments --get bench_get_binary_100pct.csv --ops-per-sample 1000 --get-success-percentage 1.0

cargo build --release --bin experiments --features uniform_bits
./target/release/experiments --get bench_get_uniform_0pct.csv --ops-per-sample 1000 --get-success-percentage 0.0
./target/release/experiments --get bench_get_uniform_50pct.csv --ops-per-sample 1000 --get-success-percentage 0.5
./target/release/experiments --get bench_get_uniform_100pct.csv --ops-per-sample 1000 --get-success-percentage 1.0
