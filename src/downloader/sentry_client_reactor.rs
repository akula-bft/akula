use crate::downloader::{
    messages::{EthMessageId, Message},
    sentry_client::*,
};
use anyhow::bail;
use futures_core::Stream;
use futures_util::TryStreamExt;
use parking_lot::RwLock;
use std::{collections::HashMap, fmt, pin::Pin, sync::Arc};
use strum::IntoEnumIterator;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use tracing::*;

pub struct SentryClientReactor {
    send_message_sender: mpsc::Sender<SendMessageCommand>,
    receive_messages_senders: Arc<RwLock<HashMap<EthMessageId, broadcast::Sender<Message>>>>,
    event_loop: Option<SentryClientReactorEventLoop>,
    event_loop_handle: Option<JoinHandle<()>>,
    stop_signal_sender: mpsc::Sender<()>,
}

struct SentryClientReactorEventLoop {
    sentry: Box<dyn SentryClient>,
    send_message_receiver: mpsc::Receiver<SendMessageCommand>,
    receive_messages_senders: Arc<RwLock<HashMap<EthMessageId, broadcast::Sender<Message>>>>,
    stop_signal_receiver: mpsc::Receiver<()>,
}

#[derive(Debug)]
struct SendMessageCommand {
    message: Message,
    peer_filter: PeerFilter,
}

#[derive(Debug)]
pub enum SendMessageError {
    SendQueueFull,
    ReactorStopped,
}

impl fmt::Display for SendMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for SendMessageError {}

impl SentryClientReactor {
    pub fn new(sentry: Box<dyn SentryClient>) -> Self {
        let (send_message_sender, send_message_receiver) =
            mpsc::channel::<SendMessageCommand>(1024);

        let mut receive_messages_senders =
            HashMap::<EthMessageId, broadcast::Sender<Message>>::new();
        for id in EthMessageId::iter() {
            let (sender, _) = broadcast::channel::<Message>(1024);
            receive_messages_senders.insert(id, sender);
        }
        let receive_messages_senders = Arc::new(RwLock::new(receive_messages_senders));

        let (stop_signal_sender, stop_signal_receiver) = mpsc::channel::<()>(1);

        let event_loop = SentryClientReactorEventLoop {
            sentry,
            send_message_receiver,
            receive_messages_senders: Arc::clone(&receive_messages_senders),
            stop_signal_receiver,
        };

        Self {
            send_message_sender,
            receive_messages_senders: Arc::clone(&receive_messages_senders),
            event_loop: Some(event_loop),
            event_loop_handle: None,
            stop_signal_sender,
        }
    }

    pub fn start(&mut self) {
        let mut event_loop = self.event_loop.take().unwrap();
        let handle = tokio::spawn(async move {
            let result = event_loop.run().await;
            if let Err(error) = result {
                error!("SentryClientReactor loop died: {:?}", error);
            }
        });
        self.event_loop_handle = Some(handle);
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(handle) = self.event_loop_handle.take() {
            self.send_stop_signal();
            handle.await?;
        }
        Ok(())
    }

    fn send_stop_signal(&self) {
        if self.stop_signal_sender.try_send(()).is_err() {
            warn!("SentryClientReactor stop signal already sent or the loop died itself");
        }
    }

    pub async fn send_message(
        &self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        let command = SendMessageCommand {
            message,
            peer_filter,
        };
        if self.send_message_sender.send(command).await.is_err() {
            bail!("Reactor stopped");
        }

        Ok(())
    }

    pub fn try_send_message(
        &self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        let command = SendMessageCommand {
            message,
            peer_filter,
        };
        let result = self.send_message_sender.try_send(command);
        match result {
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(anyhow::Error::new(SendMessageError::SendQueueFull))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::Error::new(SendMessageError::ReactorStopped))
            }
            Ok(_) => Ok(()),
        }
    }

    pub fn receive_messages(
        &self,
        filter_id: EthMessageId,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Message> + Send>>> {
        let receiver = self
            .receive_messages_senders
            .read()
            .get(&filter_id)
            .ok_or_else(|| {
                anyhow::anyhow!("SentryClientReactor unexpected filter_id {:?}", filter_id)
            })?
            .subscribe();

        let stream = BroadcastStream::new(receiver)
            .map_err(|error| match error {
                BroadcastStreamRecvError::Lagged(skipped_count) => {
                    warn!(
                        "SentryClientReactor receiver lagged too far behind, skipping {} messages",
                        skipped_count
                    );
                }
            })
            // ignore errors (logged above)
            .filter_map(|result| result.ok());

        Ok(Box::pin(stream))
    }
}

impl Drop for SentryClientReactor {
    fn drop(&mut self) {
        if self.event_loop_handle.is_some() {
            self.send_stop_signal();
        }
    }
}

impl SentryClientReactorEventLoop {
    async fn run(&mut self) -> anyhow::Result<()> {
        // subscribe to incoming messages
        let mut in_stream = self.sentry.receive_messages(&[]).await?;

        loop {
            tokio::select! {
                Some(command) = self.send_message_receiver.recv() => {
                    let send_result = self.sentry.send_message(command.message, command.peer_filter).await;
                    match send_result {
                        Ok(sent_peers_count) => {
                            debug!("SentryClientReactor.EventLoop sent message to {:?} peers", sent_peers_count);
                        }
                        Err(error) => {
                            error!("SentryClientReactor.EventLoop sentry.send_message error: {}", error);
                        }
                    }
                }
                message_result = in_stream.next() => {
                    match message_result {
                        Some(Ok(message_from_peer)) => {
                            let id = message_from_peer.message.eth_id();
                            debug!("SentryClientReactor.EventLoop incoming message: {:?}", id);

                            if self.receive_messages_senders
                                .read()
                                .get(&id)
                                .ok_or_else(|| anyhow::anyhow!("SentryClientReactor.EventLoop unexpected message id {:?}", id))?
                                .send(message_from_peer.message).is_err() {
                                debug!("SentryClientReactor.EventLoop no subscribers for message {:?}, dropping", id);
                            }
                        }
                        Some(Err(error)) => {
                            error!("SentryClientReactor.EventLoop receive message error: {}", error);
                            if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
                                if io_error.kind() == std::io::ErrorKind::BrokenPipe {
                                    info!("SentryClientReactor.EventLoop TODO: need to reconnect in_stream");
                                }
                            }
                        },
                        None => {
                            debug!("SentryClientReactor.EventLoop receive_messages stream ended");
                            break;
                        }
                    }
                }
                Some(_) = self.stop_signal_receiver.recv() => {
                    break;
                }
                else => {
                    break;
                }
            }
        }

        // drop shared senders so that existing receive_messages streams end
        self.receive_messages_senders.write().clear();

        info!("SentryClientReactor stopped");
        Ok(())
    }
}
