use crate::{
    downloader::headers::header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    kv,
    kv::traits::MutableTransaction,
};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{cell::RefCell, ops::DerefMut, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage<DB: kv::traits::MutableKV> {
    header_slices: Arc<HeaderSlices>,
    pending_watch: RefCell<watch::Receiver<usize>>,
    db: Arc<DB>,
}

impl<DB: kv::traits::MutableKV> SaveStage<DB> {
    pub fn new(header_slices: Arc<HeaderSlices>, db: Arc<DB>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Verified);

        Self {
            header_slices,
            pending_watch: RefCell::new(pending_watch),
            db,
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Verified)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("SaveStage: start");
        if self.pending_count() == 0 {
            debug!("SaveStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
            }
            debug!("SaveStage: waiting pending done");
        }

        info!("SaveStage: saving {} slices", self.pending_count());
        self.save_pending().await?;
        debug!("SaveStage: done");
        Ok(())
    }

    async fn save_pending(&self) -> anyhow::Result<()> {
        while let Some(slice_lock) = self
            .header_slices
            .find_by_status(HeaderSliceStatus::Verified)
        {
            let slice = slice_lock.upgradable_read();
            self.save_slice(&slice).await?;

            let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
            self.header_slices
                .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Saved);
        }
        Ok(())
    }

    async fn save_slice(&self, slice: &HeaderSlice) -> anyhow::Result<()> {
        let tx = self.db.begin_mutable().await?;
        if let Some(headers) = slice.headers.as_ref() {
            for header in headers {
                tx.set(
                    &kv::tables::Header,
                    ((header.number, header.hash()), header.clone()),
                )
                .await?;
            }
        }
        tx.commit().await
    }
}
