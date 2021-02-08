#![feature(
    destructuring_assignment,
    generic_associated_types,
    int_bits_const,
    trait_alias,
    type_alias_impl_trait
)]
#![allow(
    incomplete_features,
    clippy::mutable_key_type,
    clippy::unused_io_amount
)]

mod changeset;
mod common;
mod dbutils;
mod ext;
mod interface;
mod kv_mdbx;
mod models;
mod object_db;
mod remote;
mod state;
mod traits;

pub use changeset::ChangeSet;
pub use dbutils::{buckets, Bucket, DupSort, SyncStage};
pub use ext::TransactionExt;
pub use kv_mdbx::*;
pub use object_db::*;
pub use remote::{kv_client::KvClient as RemoteKvClient, RemoteCursor, RemoteTransaction};
pub use state::*;
pub use traits::{
    ComparatorFunc, Cursor, CursorDupFixed, CursorDupFixed2, CursorDupSort, CursorDupSort2,
    MutableCursor, Transaction, Transaction2,
};
