# Akula
Ethereum client written in Rust, based on [Erigon client architecture](https://github.com/ledgerwatch/interfaces).

## Building the source

Install `rustup` from rustup.rs.

```
git clone https://github.com/akula-bft/akula

cd akula

cargo build --all --release
```

You can find built binaries in `target/release` folder.

## Running

* `akula` takes an _already synced_ [Erigon](https://github.com/ledgerwatch/erigon) database with downloaded blocks and headers (stages 1-3), imports them, executes and verifies state root:

```
akula --chaindata=<path to Akula chaindata> --erigon-chaindata=<path to Erigon chaindata folder>
```

* `akula-toolbox` provides various helper commands to check and manipulate Akula's chaindata. Please consult its help for more info:
```
akula-toolbox --help
```
