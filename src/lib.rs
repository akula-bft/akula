#![feature(
    adt_const_params,
    assert_matches,
    const_for,
    const_mut_refs,
    entry_insert,
    generator_trait,
    generators,
    iter_collect_into,
    let_else,
    map_first_last,
    never_type,
    poll_ready,
    slice_swap_unchecked,
    step_trait
)]
#![recursion_limit = "256"]
#![allow(
    dead_code,
    incomplete_features,
    clippy::mutable_key_type,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unused_io_amount
)]
#![doc = include_str!("../README.md")]

extern crate ethabi;
extern crate ethereum_types;
extern crate lru_cache;
extern crate parking_lot;
extern crate keccak_hash as hash;
extern crate rlp;
extern crate parity_bytes as parbytes;
#[macro_use]
extern crate lazy_static;
extern crate secp256k1;
extern crate ethabi_derive;
extern crate rustc_hex;
#[macro_use]
extern crate ethabi_contract;

pub mod accessors;
#[doc(hidden)]
pub mod binutil;
mod bitmapdb;
pub mod chain;
pub mod consensus;
pub mod crypto;
pub mod etl;
pub mod execution;
pub mod kv;
pub mod models;
pub mod p2p;
pub mod res;
pub mod rpc;
pub mod sentry;
pub mod stagedsync;
pub mod stages;
mod state;
pub mod trie;
pub(crate) mod util;

pub use stagedsync::stage::StageId;
pub use state::*;
pub use util::*;
