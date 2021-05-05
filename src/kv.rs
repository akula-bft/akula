pub mod mdbx;
pub mod remote;
pub mod traits;

use ::mdbx::{Geometry, WriteMap};
use async_trait::async_trait;
use byte_unit::n_mb_bytes;

pub struct MemoryKv {
    inner: mdbx::Environment<WriteMap>,
    _tmpdir: tempfile::TempDir,
}

#[async_trait(?Send)]
impl traits::KV for MemoryKv {
    type Tx<'tx> = <mdbx::Environment<WriteMap> as traits::KV>::Tx<'tx>;

    async fn begin(&self, flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        self.inner.begin(flags).await
    }
}

#[async_trait(?Send)]
impl traits::MutableKV for MemoryKv {
    type MutableTx<'tx> = <mdbx::Environment<WriteMap> as traits::MutableKV>::MutableTx<'tx>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        self.inner.begin_mutable().await
    }
}

pub fn new_mem_database() -> anyhow::Result<impl traits::MutableKV> {
    let tmpdir = tempfile::tempdir()?;
    let mut builder = ::mdbx::GenericEnvironment::<WriteMap>::new();
    builder.set_max_dbs(crate::tables::TABLE_MAP.len());
    builder.set_geometry(Geometry {
        size: Some(0..n_mb_bytes!(64) as usize),
        growth_step: None,
        shrink_threshold: None,
        page_size: None,
    });
    let inner = mdbx::Environment::open(builder, tmpdir.path(), &crate::tables::TABLE_MAP)?;

    Ok(MemoryKv {
        inner,
        _tmpdir: tmpdir,
    })
}
