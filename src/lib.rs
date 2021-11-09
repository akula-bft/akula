#![feature(
    bool_to_option,
    destructuring_assignment,
    entry_insert,
    generic_associated_types,
    linked_list_cursors,
    never_type,
    step_trait,
    type_alias_impl_trait
)]
#![recursion_limit = "256"]
#![allow(
    dead_code,
    incomplete_features,
    clippy::mutable_key_type,
    clippy::type_complexity,
    clippy::unused_io_amount
)]

pub mod accessors;
pub mod adapter;
mod bitmapdb;
pub mod chain;
mod changeset;
pub mod consensus;
pub mod crypto;
pub mod downloader;
pub mod etl;
pub mod execution;
pub mod kv;
pub mod models;
pub mod res;
pub mod sentry;
pub mod stagedsync;
pub mod stages;
mod state;
pub(crate) mod util;

pub use changeset::*;
pub use kv::{
    mdbx::Environment as MdbxEnvironment,
    new_mem_database,
    remote::{kv_client::KvClient as RemoteKvClient, RemoteCursor, RemoteTransaction},
    traits::{
        Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort, MutableTransaction, Transaction,
    },
};
pub use stagedsync::stages::StageId;
pub use state::*;
pub use util::*;
