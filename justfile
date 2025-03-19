help:
    just -l

dump:
    NOZZLE_CONFIG=$HOME/.config/nozzle/config.toml cargo run --release -p nozzle -- dump --dataset eth_firehose -e 1000000 -j 4
