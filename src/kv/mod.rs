pub mod mdbx;
pub mod remote;
pub mod server;
pub mod traits;

use ::mdbx::{Geometry, WriteMap};
use async_trait::async_trait;
use byte_unit::n_mb_bytes;
use static_bytes::Bytes as StaticBytes;
use std::fmt::Debug;

pub trait Table: Send + Sync + Debug + 'static {
    fn db_name(&self) -> string::String<StaticBytes>;
}

pub trait DupSort: Table {}

#[derive(Debug)]
pub struct CustomTable(pub string::String<StaticBytes>);

impl Table for CustomTable {
    fn db_name(&self) -> string::String<StaticBytes> {
        self.0.clone()
    }
}

impl From<String> for CustomTable {
    fn from(s: String) -> Self {
        Self(unsafe { string::String::from_utf8_unchecked(StaticBytes::from(s.into_bytes())) })
    }
}

impl DupSort for CustomTable {}

pub mod tables {
    include!(concat!(env!("OUT_DIR"), "/tables.rs"));
}

pub struct MemoryKv {
    inner: mdbx::Environment<WriteMap>,
    _tmpdir: tempfile::TempDir,
}

#[async_trait]
impl traits::KV for MemoryKv {
    type Tx<'tx> = <mdbx::Environment<WriteMap> as traits::KV>::Tx<'tx>;

    async fn begin(&self, flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        self.inner.begin(flags).await
    }
}

#[async_trait]
impl traits::MutableKV for MemoryKv {
    type MutableTx<'tx> = <mdbx::Environment<WriteMap> as traits::MutableKV>::MutableTx<'tx>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        self.inner.begin_mutable().await
    }
}

pub fn new_mem_database() -> anyhow::Result<impl traits::MutableKV> {
    let tmpdir = tempfile::tempdir()?;
    let mut builder = ::mdbx::Environment::<WriteMap>::new();
    builder.set_max_dbs(tables::TABLE_MAP.len());
    builder.set_geometry(Geometry {
        size: Some(0..n_mb_bytes!(64) as usize),
        growth_step: None,
        shrink_threshold: None,
        page_size: None,
    });
    let inner = mdbx::Environment::open_rw(builder, tmpdir.path(), &tables::TABLE_MAP)?;

    Ok(MemoryKv {
        inner,
        _tmpdir: tmpdir,
    })
}
