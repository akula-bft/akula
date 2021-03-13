#![feature(
    bool_to_option,
    destructuring_assignment,
    generic_associated_types,
    int_bits_const,
    specialization,
    trait_alias,
    type_alias_impl_trait
)]
#![allow(
    incomplete_features,
    clippy::mutable_key_type,
    clippy::unused_io_amount
)]

pub mod accessors;
pub mod adapter;
mod changeset;
mod common;
mod dbutils;
mod interface;
mod kv_mdbx;
mod models;
mod object_db;
mod remote;
pub mod stagedsync;
mod state;
mod traits;
pub mod txdb;

pub use changeset::ChangeSet;
pub use dbutils::{buckets, Bucket, DupSort, SyncStage};
pub use kv_mdbx::*;
pub use object_db::*;
pub use remote::{kv_client::KvClient as RemoteKvClient, RemoteCursor, RemoteTransaction};
pub use state::*;
pub use traits::{
    txutil, ComparatorFunc, Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort,
    MutableTransaction, Transaction,
};
