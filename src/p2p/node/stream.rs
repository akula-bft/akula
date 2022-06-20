use super::Sentry;
use crate::p2p::types::InboundMessage;
use ethereum_interfaces::sentry::{self as grpc_sentry, PenalizePeerRequest};
use futures::Stream;
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

pub struct SentryStream;

pub type NodeStream = Pin<Box<dyn Stream<Item = InboundMessage> + Send>>;

impl SentryStream {
    const BACKOFF: Duration = Duration::from_millis(100);

    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        sentry: &Sentry,
        sentry_id: usize,
        pred: Vec<i32>,
    ) -> anyhow::Result<NodeStream> {
        let (penalize_tx, mut penalize_rx) = mpsc::channel(4);
        tokio::task::spawn({
            let mut sentry = sentry.clone();
            async move {
                while let Some(peer_id) = penalize_rx.recv().await {
                    let _ = sentry
                        .penalize_peer(PenalizePeerRequest {
                            peer_id,
                            penalty: 0i32,
                        })
                        .await;
                }
            }
        });

        let stream = {
            let mut sentry = sentry.clone();
            let mut inner_stream = sentry
                .messages(grpc_sentry::MessagesRequest { ids: pred })
                .await?
                .into_inner();

            Box::pin(async_stream::stream! {
                loop {
                    if let Some(Ok(msg)) = inner_stream.next().await {
                        let peer_id = msg.peer_id.clone();

                        if let Ok(msg) = InboundMessage::new(msg, sentry_id) {
                            yield msg;
                        } else {
                            let _ = penalize_tx.send(peer_id).await;
                        }
                    }
                }
            })
        };

        Ok::<_, anyhow::Error>(stream)
    }

    pub async fn join_all<'sentry, T, P>(iter: T, pred: P) -> NodeStream
    where
        T: IntoIterator<Item = &'sentry Sentry>,
        P: IntoIterator<Item = i32>,
    {
        let pred = pred.into_iter().collect::<Vec<_>>();

        Box::pin(futures::stream::select_all(
            futures::future::join_all(
                iter.into_iter()
                    .enumerate()
                    .map(|(sentry_id, sentry)| Self::new(sentry, sentry_id, pred.clone()))
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .filter_map(Result::ok),
        ))
    }
}
