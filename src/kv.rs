pub mod mdbx;
pub mod remote;
pub mod traits;

use ::mdbx::WriteMap;
use async_trait::async_trait;

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
    let inner = mdbx::Environment::open(builder, tmpdir.path(), &crate::tables::TABLE_MAP)?;

    Ok(MemoryKv {
        inner,
        _tmpdir: tmpdir,
    })
}
