use crate::{buckets, kv::traits::KV, Cursor, Transaction};
use bytes::Bytes;
use std::{marker::PhantomData, str::FromStr};

pub struct ObjectDatabase<K: KV> {
    kv: K,
}

impl From<mdbx::Environment> for ObjectDatabase<mdbx::Environment> {
    fn from(kv: mdbx::Environment) -> Self {
        Self { kv }
    }
}
