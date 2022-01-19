use super::headers::{
    header::BlockHeader,
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use crate::{
    kv,
    kv::{tables::HeaderKey, traits::MutableTransaction},
};
use anyhow::format_err;
use parking_lot::RwLock;
use std::{
    ops::{ControlFlow, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::*;

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage<'tx, RwTx> {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: Arc<AtomicUsize>,
    db_transaction: &'tx RwTx,
}

impl<'tx, 'db: 'tx, RwTx: MutableTransaction<'db>> SaveStage<'tx, RwTx> {
    pub fn new(header_slices: Arc<HeaderSlices>, db_transaction: &'tx RwTx) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Verified,
                header_slices,
                "SaveStage",
            ),
            remaining_count: Arc::new(AtomicUsize::new(0)),
            db_transaction,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // initially remaining_count = 0, so we wait for any verified slices to try to save them
        // since we want to save headers sequentially, there might be some remaining slices
        // in this case we wait until some more slices become verified
        // hopefully its the slices at the front so that we can save them
        self.pending_watch
            .wait_while(self.get_remaining_count())
            .await?;

        let pending_count = self.pending_watch.pending_count();

        debug!("SaveStage: saving {} slices", pending_count);
        let saved_count = self.save_pending_monotonic(pending_count).await?;
        debug!("SaveStage: saved {} slices", saved_count);

        self.set_remaining_count(pending_count - saved_count);

        Ok(())
    }

    fn get_remaining_count(&self) -> usize {
        self.remaining_count.load(Ordering::SeqCst)
    }

    fn set_remaining_count(&self, value: usize) {
        self.remaining_count.store(value, Ordering::SeqCst);
    }

    pub fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        let header_slices = self.header_slices.clone();
        let remaining_count = self.remaining_count.clone();
        let check = move || -> bool {
            header_slices.count_slices_in_status(HeaderSliceStatus::Verified)
                != remaining_count.load(Ordering::SeqCst)
        };
        Box::new(check)
    }

    async fn save_pending_monotonic(&mut self, pending_count: usize) -> anyhow::Result<usize> {
        let mut saved_count: usize = 0;
        for _ in 0..pending_count {
            let next_slice_lock = self.find_next_pending_monotonic();

            if let Some(slice_lock) = next_slice_lock {
                self.save_slice(slice_lock).await?;
                saved_count += 1;
            } else {
                break;
            }
        }
        Ok(saved_count)
    }

    fn find_next_pending_monotonic(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        let initial_value = Option::<Arc<RwLock<HeaderSlice>>>::None;
        let next_slice_lock = self.header_slices.try_fold(initial_value, |_, slice_lock| {
            let slice = slice_lock.read();
            match slice.status {
                HeaderSliceStatus::Saved => ControlFlow::Continue(None),
                HeaderSliceStatus::Verified => ControlFlow::Break(Some(slice_lock.clone())),
                _ => ControlFlow::Break(None),
            }
        });

        if let ControlFlow::Break(slice_lock_opt) = next_slice_lock {
            slice_lock_opt
        } else {
            None
        }
    }

    // this is kept for performance comparison with save_pending_monotonic
    async fn save_pending_all(&self, _pending_count: usize) -> anyhow::Result<usize> {
        let mut saved_count: usize = 0;
        while let Some(slice_lock) = self
            .header_slices
            .find_by_status(HeaderSliceStatus::Verified)
        {
            self.save_slice(slice_lock).await?;
            saved_count += 1;
        }
        Ok(saved_count)
    }

    async fn save_slice(&self, slice_lock: Arc<RwLock<HeaderSlice>>) -> anyhow::Result<()> {
        // take out the headers, and unlock the slice while save_slice is in progress
        let headers = {
            let mut slice = slice_lock.write();
            slice.headers.take().ok_or_else(|| {
                format_err!("SaveStage: inconsistent state - Verified slice has no headers")
            })?
        };

        self.save_headers(&headers).await?;

        let mut slice = slice_lock.write();

        // put the detached headers back
        slice.headers = Some(headers);

        self.header_slices
            .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Saved);
        Ok(())
    }

    async fn save_headers(&self, headers: &[BlockHeader]) -> anyhow::Result<()> {
        let tx = &self.db_transaction;
        for header_ref in headers {
            // this clone happens mostly on the stack (except extra_data)
            let header = header_ref.clone();
            self.save_header(header, tx).await?;
        }
        Ok(())
    }

    async fn save_header(&self, header: BlockHeader, tx: &RwTx) -> anyhow::Result<()> {
        let block_num = header.number();
        let header_hash = header.hash();
        let header_key: HeaderKey = (block_num, header_hash);
        let total_difficulty = header.difficulty();

        // saving a precomputed RLP representation
        tx.set(HeaderTableWithBytes, header_key, header.rlp_repr())
            .await?;
        tx.set(kv::tables::HeaderNumber, header_hash, block_num)
            .await?;
        tx.set(kv::tables::CanonicalHeader, block_num, header_hash)
            .await?;
        tx.set(
            kv::tables::HeadersTotalDifficulty,
            header_key,
            total_difficulty,
        )
        .await?;
        tx.set(kv::tables::LastHeader, Default::default(), header_hash)
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
struct HeaderTableWithBytes;

impl kv::traits::Table for HeaderTableWithBytes {
    type Key = <kv::tables::Header as kv::traits::Table>::Key;
    type Value = bytes::Bytes;
    type SeekKey = <kv::tables::Header as kv::traits::Table>::SeekKey;

    fn db_name(&self) -> string::String<bytes::Bytes> {
        let table = kv::tables::Header;
        table.db_name()
    }
}

#[async_trait::async_trait]
impl<'tx, 'db: 'tx, RwTx: MutableTransaction<'db>> super::stage::Stage for SaveStage<'tx, RwTx> {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Self::can_proceed_check(self)
    }
}
