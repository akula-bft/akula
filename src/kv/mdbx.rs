use crate::{
    kv::traits, Cursor, CursorDupSort, DupSort, MutableCursor, MutableCursorDupSort, Table,
};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use mdbx::{
    Cursor as MdbxCursor, EnvironmentKind, Error as MdbxError, GenericEnvironment,
    Transaction as MdbxTransaction, TransactionKind, WriteFlags, WriteMap, RO, RW,
};

fn filter_not_found<T>(res: Result<T, mdbx::Error>) -> anyhow::Result<Option<T>> {
    match res {
        Ok(v) => Ok(Some(v)),
        Err(MdbxError::NotFound) => Ok(None),
        Err(other) => Err(other.into()),
    }
}

fn set<'txn, K: TransactionKind>(
    c: &mut MdbxCursor<'txn, K>,
    k: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
    filter_not_found(MdbxCursor::set_key(c, k))
}

fn get_both_range<'txn, K: TransactionKind>(
    c: &mut MdbxCursor<'txn, K>,
    k: &[u8],
    v: &[u8],
) -> anyhow::Result<Option<Bytes<'txn>>> {
    filter_not_found(MdbxCursor::get_both_range(c, k, v))
}

#[async_trait(?Send)]
impl<E: EnvironmentKind> traits::KV for GenericEnvironment<E> {
    type Tx<'tx> = MdbxTransaction<'tx, RO, E>;

    async fn begin(&self, _flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        Ok(self.begin_ro_txn()?)
    }
}

#[async_trait(?Send)]
impl<E: EnvironmentKind> traits::MutableKV for GenericEnvironment<E> {
    type MutableTx<'tx> = MdbxTransaction<'tx, RW, E>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(self.begin_rw_txn()?)
    }
}

#[async_trait(?Send)]
impl<'env: 'tx, 'tx, K, E> traits::Transaction<'tx> for MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    type Cursor<B: Table> = MdbxCursor<'tx, K>;
    type CursorDupSort<B: DupSort> = MdbxCursor<'tx, K>;

    async fn cursor<B: Table>(&'tx self) -> anyhow::Result<Self::Cursor<B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.cursor()?)
    }

    async fn cursor_dup_sort<B: DupSort>(&'tx self) -> anyhow::Result<Self::Cursor<B>> {
        self.cursor::<B>().await
    }
}

#[async_trait(?Send)]
impl<'env: 'tx, 'tx, E: EnvironmentKind> traits::MutableTransaction<'tx>
    for MdbxTransaction<'env, RW, E>
{
    type MutableCursor<B: Table> = MdbxCursor<'tx, RW>;

    async fn mutable_cursor<B: Table>(&'tx self) -> anyhow::Result<Self::MutableCursor<B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.cursor()?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        MdbxTransaction::commit(self)?;

        Ok(())
    }

    async fn table_size<B: Table>(&self) -> anyhow::Result<u64> {
        let st = self.open_db(Some(B::DB_NAME))?.stat()?;

        Ok(
            ((st.leaf_pages() + st.branch_pages() + st.overflow_pages()) * st.page_size() as usize)
                as u64,
        )
    }

    async fn sequence<B: Table>(&self, amount: usize) -> anyhow::Result<usize> {
        let mut c = self.mutable_cursor::<B>().await?;

        let current_v = Cursor::<Self, B>::seek_exact(&mut c, B::DB_NAME.as_bytes())
            .await?
            .map(|(k, v)| usize::from_be_bytes(*array_ref!(v, 0, 8)))
            .unwrap_or(0);

        if amount == 0 {
            return Ok(current_v);
        }

        MutableCursor::<Self, B>::put(
            &mut c,
            B::DB_NAME.as_bytes(),
            &(current_v + amount).to_be_bytes(),
        )
        .await?;

        Ok(current_v)
    }
}

#[async_trait(?Send)]
impl<'env: 'txn, 'txn, K, E, B> Cursor<'txn, MdbxTransaction<'env, K, E>, B> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    E: EnvironmentKind,
    B: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        filter_not_found(MdbxCursor::first(self))
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn seek_exact(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        set(self, key)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'env: 'txn, 'txn, K, E, B> CursorDupSort<'txn, MdbxTransaction<'env, K, E>, B>
    for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    E: EnvironmentKind,
    B: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<Bytes<'txn>>> {
        get_both_range(self, key, value)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        filter_not_found(MdbxCursor::next_dup(self))
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        filter_not_found(MdbxCursor::next_nodup(self))
    }
}

#[async_trait(?Send)]
impl<'env: 'txn, 'txn, E, B> MutableCursor<'txn, MdbxTransaction<'env, RW, E>, B>
    for MdbxCursor<'txn, RW>
where
    E: EnvironmentKind,
    B: Table,
{
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.del(Default::default())?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'env: 'txn, 'txn, E, B> MutableCursorDupSort<'txn, MdbxTransaction<'env, RW, E>, B>
    for MdbxCursor<'txn, RW>
where
    E: EnvironmentKind,
    B: DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.del(WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND_DUP)?)
    }
}
