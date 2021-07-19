#![feature(
    bool_to_option,
    destructuring_assignment,
    entry_insert,
    generic_associated_types,
    never_type,
    type_alias_impl_trait
)]
#![recursion_limit = "256"]
#![allow(
    dead_code,
    incomplete_features,
    clippy::mutable_key_type,
    clippy::unused_io_amount
)]

pub mod accessors;
pub mod adapter;
mod bitmapdb;
pub mod chain;
mod changeset;
mod common;
mod crypto;
mod dbutils;
pub mod downloader;
pub mod etl;
pub mod execution;
pub mod kv;
mod models;
pub mod stagedsync;
pub mod stages;
mod state;
pub mod txdb;
pub(crate) mod util;

pub use changeset::*;
pub use kv::{
    mdbx::{table_sizes as mdbx_table_sizes, Environment as MdbxEnvironment},
    new_mem_database,
    remote::{kv_client::KvClient as RemoteKvClient, RemoteCursor, RemoteTransaction},
    traits::{
        Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort, MutableTransaction, Transaction,
    },
};
pub use stagedsync::stages::StageId;
pub use state::*;
