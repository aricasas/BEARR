CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release
perf record -e task-clock -F 99 --call-graph dwarf --  ./target/release/experiments --get bench_get_T4.csv --ops-per-sample 1000 --size-ratio 4 --get-success-percentage 1.0 --total-entries 20000000
perf script | inferno-collapse-perf | inferno-flamegraph > "flamegraph_$1.svg"
