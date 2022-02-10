pub mod mdbx;
pub mod tables;
pub mod traits;

use self::traits::*;
use crate::kv::tables::CHAINDATA_TABLES;
use ::mdbx::{Geometry, WriteMap};
use byte_unit::*;
use bytes::Bytes;
use derive_more::Deref;
use std::{fmt::Debug, ops::Deref};

#[derive(Debug)]
pub struct CustomTable(pub string::String<Bytes>);

impl Table for CustomTable {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type SeekKey = Vec<u8>;

    fn db_name(&self) -> string::String<Bytes> {
        self.0.clone()
    }
}

impl From<String> for CustomTable {
    fn from(s: String) -> Self {
        Self(unsafe { string::String::from_utf8_unchecked(Bytes::from(s.into_bytes())) })
    }
}

impl DupSort for CustomTable {
    type SeekBothKey = Vec<u8>;
}

#[derive(Debug, Deref)]
pub struct MdbxWithDirHandle {
    #[deref]
    inner: mdbx::MdbxEnvironment<WriteMap>,
    _tmpdir: Option<tempfile::TempDir>,
}

pub fn new_mem_database() -> anyhow::Result<MdbxWithDirHandle> {
    let tmpdir = tempfile::tempdir()?;
    Ok(MdbxWithDirHandle {
        inner: new_environment(tmpdir.path(), n_mib_bytes!(64), None)?,
        _tmpdir: Some(tmpdir),
    })
}

pub fn new_database(path: &std::path::Path) -> anyhow::Result<MdbxWithDirHandle> {
    Ok(MdbxWithDirHandle {
        inner: new_environment(path, n_tib_bytes!(4), Some(n_gib_bytes!(4) as usize))?,
        _tmpdir: None,
    })
}

fn new_environment(
    path: &std::path::Path,
    size_upper_limit: u128,
    growth_step: Option<usize>,
) -> anyhow::Result<mdbx::MdbxEnvironment<WriteMap>> {
    let mut builder = ::mdbx::Environment::<WriteMap>::new();
    builder.set_max_dbs(CHAINDATA_TABLES.len());
    builder.set_geometry(Geometry {
        size: Some(0..size_upper_limit.try_into().unwrap_or(usize::MAX)),
        growth_step: growth_step.map(|s| s.try_into().unwrap_or(isize::MAX)),
        shrink_threshold: None,
        page_size: None,
    });
    builder.set_rp_augment_limit(16 * 256 * 1024);
    mdbx::MdbxEnvironment::open_rw(builder, path, CHAINDATA_TABLES.deref().clone())
}
