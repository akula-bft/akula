use crate::{
    downloader::headers::{
        header_slice_status_watch::HeaderSliceStatusWatch,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    kv,
    kv::traits::MutableTransaction,
};
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc};
use tracing::*;

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage<DB: kv::traits::MutableKV + Sync> {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
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
            db,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("SaveStage: start");
        self.pending_watch.wait().await?;

        info!(
            "SaveStage: saving {} slices",
            self.pending_watch.pending_count()
        );
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

#[async_trait::async_trait]
impl<DB: kv::traits::MutableKV + Sync> super::stage::Stage for SaveStage<DB> {
    async fn execute(&mut self) -> anyhow::Result<()> {
        SaveStage::<DB>::execute(self).await
    }
}
