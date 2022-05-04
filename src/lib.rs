#![feature(
    async_closure,
    adt_const_params,
    bool_to_option,
    const_for,
    const_mut_refs,
    const_trait_impl,
    const_convert,
    entry_insert,
    generator_trait,
    generators,
    let_else,
    map_first_last,
    never_type,
    poll_ready,
    slice_swap_unchecked,
    step_trait,
    type_alias_impl_trait,
    iter_collect_into
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

pub use stagedsync::stages::StageId;
pub use state::*;
pub use util::*;
