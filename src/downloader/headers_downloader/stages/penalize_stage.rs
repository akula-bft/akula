use super::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use crate::sentry::{sentry_client::PeerId, sentry_client_reactor::*};
use parking_lot::RwLockUpgradableReadGuard;
use std::{collections::HashSet, ops::DerefMut, sync::Arc};
use tracing::*;

/// Penalize peers for sending us headers that failed to verify, and mark the related slices as Empty for retry.
pub struct PenalizeStage {
    header_slices: Arc<HeaderSlices>,
    sentry: SentryClientReactorShared,
    pending_watch: HeaderSliceStatusWatch,
}

impl PenalizeStage {
    pub fn new(header_slices: Arc<HeaderSlices>, sentry: SentryClientReactorShared) -> Self {
        Self {
            header_slices: header_slices.clone(),
            sentry,
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Invalid,
                header_slices,
                "PenalizeStage",
            ),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.pending_watch.wait().await?;

        debug!(
            "PenalizeStage: processing {} invalid slices",
            self.pending_watch.pending_count()
        );

        let bad_peers = self.collect_bad_peers()?;
        warn!(
            "PenalizeStage: penalizing {} bad peers: {:?}",
            bad_peers.len(),
            bad_peers
        );
        self.penalize_peers(bad_peers).await?;
        self.reset_pending();

        Ok(())
    }

    fn collect_bad_peers(&self) -> anyhow::Result<HashSet<PeerId>> {
        let mut peers = HashSet::<PeerId>::new();
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.read();
            if slice.status == HeaderSliceStatus::Invalid {
                match slice.from_peer_id {
                    Some(from_peer_id) => { peers.insert(from_peer_id); }
                    None => warn!("PenalizeStage: got an invalid headers slice from an unknown peer starting at: {:?}", slice.start_block_num),
                }
            }
        });
        Ok(peers)
    }

    fn reset_pending(&self) {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Invalid {
                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                self.header_slices
                    .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Empty);
                slice.headers = None;
            }
        });
    }

    async fn penalize_peers(&self, peers: HashSet<PeerId>) -> anyhow::Result<()> {
        let sentry = self.sentry.read().await;
        for peer_id in peers {
            sentry.penalize_peer(peer_id).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for PenalizeStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
}
