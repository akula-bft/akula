use crate::downloader::{
    messages::{EthMessageId, Message},
    sentry_client::*,
};
use futures_core::Stream;
use futures_util::TryStreamExt;
use parking_lot::RwLock;
use std::{collections::HashMap, fmt, sync::Arc};
use strum::IntoEnumIterator;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};

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
                tracing::error!("SentryClientReactor loop died: {:?}", error);
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
        let result = self.stop_signal_sender.try_send(());
        if result.is_err() {
            tracing::warn!("SentryClientReactor stop signal already sent or the loop died itself");
        }
    }

    pub fn send_message(&self, message: Message, peer_filter: PeerFilter) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<Box<dyn Stream<Item = Message> + Unpin + Send>> {
        let receiver: broadcast::Receiver<Message>;
        {
            // release the lock on receive_messages_senders after getting a new receiver
            let senders = self.receive_messages_senders.read();
            let sender = senders.get(&filter_id).ok_or_else(|| {
                anyhow::anyhow!("SentryClientReactor unexpected filter_id {:?}", filter_id)
            })?;

            receiver = sender.subscribe();
        }

        let stream = BroadcastStream::new(receiver)
            .map_err(|error| match error {
                BroadcastStreamRecvError::Lagged(skipped_count) => {
                    tracing::warn!(
                        "SentryClientReactor receiver lagged too far behind, skipping {} messages",
                        skipped_count
                    );
                }
            })
            // ignore errors (logged above)
            .filter_map(|result| result.ok());

        Ok(Box::new(Box::pin(stream)))
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
                            tracing::debug!("SentryClientReactor.EventLoop sent message to {:?} peers", sent_peers_count);
                        }
                        Err(error) => {
                            tracing::error!("SentryClientReactor.EventLoop sentry.send_message error: {}", error);
                        }
                    }
                }
                message_result = in_stream.next() => {
                    match message_result {
                        Some(Ok(message_from_peer)) => {
                            let id = message_from_peer.message.eth_id();
                            tracing::debug!("SentryClientReactor.EventLoop incoming message: {:?}", id);

                            let senders = self.receive_messages_senders.read();
                            let sender = senders.get(&id)
                                .ok_or_else(|| anyhow::anyhow!("SentryClientReactor.EventLoop unexpected message id {:?}", id))?;
                            let send_result = sender.send(message_from_peer.message);
                            if send_result.is_err() {
                                tracing::debug!("SentryClientReactor.EventLoop no subscribers for message {:?}, dropping", id);
                            }
                        }
                        Some(Err(error)) => {
                            tracing::error!("SentryClientReactor.EventLoop receive message error: {}", error);
                            if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
                                if io_error.kind() == std::io::ErrorKind::BrokenPipe {
                                    tracing::info!("SentryClientReactor.EventLoop TODO: need to reconnect in_stream");
                                }
                            }
                        },
                        None => {
                            tracing::debug!("SentryClientReactor.EventLoop receive_messages stream ended");
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
        {
            let mut senders = self.receive_messages_senders.write();
            senders.clear();
        }

        tracing::info!("SentryClientReactor stopped");
        Ok(())
    }
}
