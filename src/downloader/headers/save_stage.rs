use crate::{
    downloader::headers::{
        header_slice_status_watch::HeaderSliceStatusWatch,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    kv,
    kv::traits::MutableTransaction,
    models::BlockHeader,
};
use anyhow::anyhow;
use parking_lot::RwLock;
use std::{ops::DerefMut, sync::Arc};
use tracing::*;

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage<DB: kv::traits::MutableKV + Sync> {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: usize,
    db: Arc<DB>,
}

impl<DB: kv::traits::MutableKV + Sync> SaveStage<DB> {
    pub fn new(header_slices: Arc<HeaderSlices>, db: Arc<DB>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Verified,
                header_slices,
                "SaveStage",
            ),
            remaining_count: 0,
            db,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("SaveStage: start");

        // initially remaining_count = 0, so we wait for any verified slices to try to save them
        // since we want to save headers sequentially, there might be some remaining slices
        // in this case we wait until some more slices become verified
        // hopefully its the slices at the front so that we can save them
        self.pending_watch.wait_while(self.remaining_count).await?;

        let pending_count = self.pending_watch.pending_count();

        debug!("SaveStage: saving {} slices", pending_count);
        let saved_count = self.save_pending_monotonic(pending_count).await?;
        debug!("SaveStage: saved {} slices", saved_count);

        self.remaining_count = pending_count - saved_count;

        debug!("SaveStage: done");
        Ok(())
    }

    async fn save_pending_monotonic(&mut self, pending_count: usize) -> anyhow::Result<usize> {
        let mut saved_count: usize = 0;
        for i in 0..pending_count {
            let slice_lock = self.header_slices.find_by_index(i).ok_or_else(|| {
                anyhow!("SaveStage: inconsistent state - less pending slices than expected")
            })?;
            let is_verified = slice_lock.read().status == HeaderSliceStatus::Verified;
            if is_verified {
                self.save_slice(slice_lock).await?;
                saved_count += 1;
            } else {
                break;
            }
        }
        Ok(saved_count)
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
                anyhow!("SaveStage: inconsistent state - Verified slice has no headers")
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
        let tx = self.db.begin_mutable().await?;
        for header_ref in headers {
            // this clone happens mostly on the stack (except extra_data)
            let header = header_ref.clone();
            tx.set(
                &kv::tables::Header,
                ((header.number, header.hash()), header),
            )
            .await?;
        }
        tx.commit().await
    }
}

#[async_trait::async_trait]
impl<DB: kv::traits::MutableKV + Sync> super::stage::Stage for SaveStage<DB> {
    async fn execute(&mut self) -> anyhow::Result<()> {
        SaveStage::<DB>::execute(self).await
    }
}
