# <h1 align="center"> 🦈 Akula 🦈 </h1>
[![Telegram Chat](https://img.shields.io/endpoint?color=neon&style=flat-square&url=https%3A%2F%2Ftg.sumanjay.workers.dev%2Fakula_bft)](https://t.me/akula_bft)

Next-generation implementation of Ethereum protocol ("client") written in Rust, based on [Erigon architecture](https://github.com/ledgerwatch/interfaces).

## Why run Akula?

Look at Mgas/s.

![](./src/res/readme-screenshot.png)


## Installation

Installation instructions available on our [website](https://akula.app/installation.html).

## Helping us and getting support

Please join [our Telegram chat](https://t.me/akula_bft) to meet the developers and find out how you can help.

Work on Akula is sponsored by:

[<img src="https://avatars.githubusercontent.com/u/24954468?s=75" alt="Gnosis">](https://gnosis.io/) [<img src="https://avatars.githubusercontent.com/u/80278162?s=75" alt="Stateful Works">](https://stateful.mirror.xyz/a151ee1decb2028a8bb48277f6928c6f38319c32601dc1da1ee82acfcad2e525)

If you'd like to sponsor, check out our [Gitcoin](https://gitcoin.co/grants/5933/akula) grant page.

---
Akula (_Акула_) is pronounced as `ah-koo-lah` and stands for _shark_ in Russian.

## License
The entire code within this repository is licensed under the [GNU General Public License v3](LICENSE)

## Dependencies

In addition to those dependency crates listed in Cargo.toml Akula also has implicit dependencies on:

- Rust/Cargo (unstable)
- pkgconfig
- clang
- libclang
- e2fsprogs
- protobuf

The latter two being used by the builds of some of Akula's dependencies.

### Building with Nix

A hermetic build/development shell environment is provided in `flake.nix` via the [nix](https://nixos.org/) package manager. It provides fixed versions of the implicit dependencies above as well as the correct version of Rust as defined in the rust `./rust-toolchain` file. You can enter the environment by running:

```shell
# Will download necessary dependencies including Rust nightly and enter a development environment where they are available
nix develop
# Dependency are now available for:
cargo build
cargo test
```
