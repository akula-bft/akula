# <h1 align="center"> 🦈 Akula 🦈 </h1>
[![Telegram Chat](https://img.shields.io/endpoint?color=neon&style=flat-square&url=https%3A%2F%2Ftg.sumanjay.workers.dev%2Fakula_bft)](https://t.me/akula_bft)

Next-generation implementation of Ethereum protocol ("client") written in Rust, based on [Erigon architecture](https://github.com/ledgerwatch/interfaces).

## Why run Akula?

Look at Mgas/s.

![](./src/res/readme-screenshot.png)


## Building the source

Requirements:
- clang 12+
- libext2fs-dev / e2fsprogs-devel

Install `rustup` from rustup.rs.

```ignore
git clone https://github.com/akula-bft/akula

cd akula

cargo build --all --profile=production
```

You can find built binaries in `target/production` folder.

## Running

* `akula` is the main binary that runs as full node, requires `akula-sentry`:

```ignore
akula --datadir=<path to Akula database directory>
```

* `akula-sentry` is the P2P node.

* `akula-toolbox` provides various helper commands to check and manipulate Akula's database. Please consult its help for more info:
```ignore
akula-toolbox --help
```

## Helping us and getting support

Please join [our Telegram chat](https://t.me/akula_bft) to meet the developers and find out how you can help.

Work on Akula is sponsored by:

[<img src="https://avatars.githubusercontent.com/u/24954468?s=75" alt="Gnosis">](https://gnosis.io/) [<img src="https://avatars.githubusercontent.com/u/80278162?s=75" alt="Stateful Works">](https://stateful.mirror.xyz/a151ee1decb2028a8bb48277f6928c6f38319c32601dc1da1ee82acfcad2e525)

If you'd like to sponsor, check out our [Gitcoin](https://gitcoin.co/grants/5933/akula) grant page.

---
Akula (_Акула_) is pronounced as `ah-koo-lah` and stands for _shark_ in Russian.

## License
The entire code within this repository is licensed under the [GNU General Public License v3](LICENSE)
