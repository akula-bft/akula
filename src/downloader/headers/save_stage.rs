use crate::{
    downloader::headers::{
        header_slice_status_watch::HeaderSliceStatusWatch,
        header_slices::{HeaderSliceStatus, HeaderSlices},
    },
    kv,
    kv::traits::MutableTransaction,
    models::BlockHeader,
};
use anyhow::anyhow;
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

        debug!(
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
            // take out the headers, and unlock the slice while save_slice is in progress
            let headers = {
                let mut slice = slice_lock.write();
                slice.headers.take().ok_or_else(|| {
                    anyhow!("SaveStage: inconsistent state - Verified slice has no headers")
                })?
            };

            self.save_slice(&headers).await?;

            let mut slice = slice_lock.write();

            // put the detached headers back
            slice.headers = Some(headers);

            self.header_slices
                .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Saved);
        }
        Ok(())
    }

    async fn save_slice(&self, headers: &[BlockHeader]) -> anyhow::Result<()> {
        let tx = self.db.begin_mutable().await?;
        for header_ref in headers {
            // TODO: heavy clone - is it possible to avoid?
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
