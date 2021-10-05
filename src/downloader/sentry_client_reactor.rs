use crate::downloader::{
    messages::{EthMessageId, Message},
    sentry_client::*,
};
use futures_core::{Future, Stream};
use futures_util::{FutureExt, TryStreamExt};
use parking_lot::RwLock;
use std::{collections::HashMap, fmt, pin::Pin, sync::Arc};
use strum::IntoEnumIterator;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, ReceiverStream},
    StreamExt, StreamMap,
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
        let (send_message_sender, send_message_receiver) = mpsc::channel::<SendMessageCommand>(1);

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

    pub fn start(&mut self) -> anyhow::Result<()> {
        let event_loop = self
            .event_loop
            .take()
            .ok_or_else(|| anyhow::anyhow!("already started once"))?;
        let handle = tokio::spawn(async move {
            let result = event_loop.run().await;
            if let Err(error) = result {
                error!("SentryClientReactor loop died: {:?}", error);
            }
        });
        self.event_loop_handle = Some(handle);
        Ok(())
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
            return Err(anyhow::Error::new(SendMessageError::ReactorStopped));
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

    pub fn reserve_capacity_in_send_queue(
        &self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>>>> {
        let sender = self.send_message_sender.clone();
        Box::pin(sender.reserve_owned().map(|result| match result {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow::Error::new(SendMessageError::ReactorStopped)),
        }))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EventLoopStreamId {
    Send,
    Receive,
    Stop,
}

#[derive(Debug)]
enum EventLoopStreamResult {
    Send(u32),
    Receive(MessageFromPeer),
    Stop(()),
}

type EventLoopStream = Pin<Box<dyn Stream<Item = anyhow::Result<EventLoopStreamResult>> + Send>>;

// dropping this causes dropping the senders and triggers an end of stream event on the receivers end
struct EventLoopReceiveMessagesSendersDropper {
    receive_messages_senders: Arc<RwLock<HashMap<EthMessageId, broadcast::Sender<Message>>>>,
}

impl Drop for EventLoopReceiveMessagesSendersDropper {
    fn drop(&mut self) {
        // drop shared senders so that existing receive_messages() streams end
        self.receive_messages_senders.write().clear();
    }
}

impl SentryClientReactorEventLoop {
    async fn run(self) -> anyhow::Result<()> {
        let mut sentry = self.sentry;

        // subscribe to incoming messages
        let mut stream = sentry.receive_messages(&[]).await?;
        let receive_messages_senders = Arc::clone(&self.receive_messages_senders);
        let receive_stream = async_stream::stream! {
            // When the stream ends (this normally happens during tests)
            // we need to ensure unblocking the subscribers which called receive_messages().
            let _receive_messages_senders_dropper = EventLoopReceiveMessagesSendersDropper { receive_messages_senders };

            while let Some(message_result) = stream.next().await {
                yield message_result;
            }

            debug!("SentryClientReactor.EventLoop receive_messages stream ended");
        };

        // When the reactor loop stops/aborts (e.g. after calling stop())
        // we need to ensure unblocking the subscribers which called receive_messages().
        let _receive_messages_senders_dropper = EventLoopReceiveMessagesSendersDropper {
            receive_messages_senders: Arc::clone(&self.receive_messages_senders),
        };

        let mut send_message_receiver = self.send_message_receiver;
        let send_stream = async_stream::stream! {
            while let Some(command) = send_message_receiver.recv().await {
                let send_result = sentry.send_message(command.message, command.peer_filter).await;
                yield send_result;
            }
        };

        let stop_stream = ReceiverStream::new(self.stop_signal_receiver);

        let mut stream = StreamMap::<EventLoopStreamId, EventLoopStream>::new();
        stream.insert(
            EventLoopStreamId::Send,
            Box::pin(send_stream.map_ok(EventLoopStreamResult::Send)),
        );
        stream.insert(
            EventLoopStreamId::Receive,
            Box::pin(receive_stream.map_ok(EventLoopStreamResult::Receive)),
        );
        stream.insert(
            EventLoopStreamId::Stop,
            Box::pin(stop_stream.map(|result| Ok(EventLoopStreamResult::Stop(result)))),
        );

        while let Some((stream_id, result)) = stream.next().await {
            match stream_id {
                EventLoopStreamId::Send => {
                    // process an outgoing message that has been just sent
                    match result {
                        Ok(EventLoopStreamResult::Send(sent_peers_count)) => {
                            debug!(
                                "SentryClientReactor.EventLoop sent message to {:?} peers",
                                sent_peers_count
                            );
                        }
                        Ok(_) => panic!("unexpected result {:?}", result),
                        Err(error) => {
                            error!(
                                "SentryClientReactor.EventLoop sentry.send_message error: {}",
                                error
                            );
                        }
                    }
                }
                EventLoopStreamId::Receive => {
                    // process an incoming message that was received
                    match result {
                        Ok(EventLoopStreamResult::Receive(message_from_peer)) => {
                            let id = message_from_peer.message.eth_id();
                            debug!("SentryClientReactor.EventLoop incoming message: {:?}", id);

                            let receive_messages_senders = self.receive_messages_senders.read();
                            let sender_opt = receive_messages_senders.get(&id);
                            let sender = sender_opt.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "SentryClientReactor.EventLoop unexpected message id {:?}",
                                    id
                                )
                            })?;

                            let send_sub_result = sender.send(message_from_peer.message);
                            if send_sub_result.is_err() {
                                debug!("SentryClientReactor.EventLoop no subscribers for message {:?}, dropping", id);
                            }
                        }
                        Ok(_) => panic!("unexpected result {:?}", result),
                        Err(error) => {
                            error!(
                                "SentryClientReactor.EventLoop receive message error: {}",
                                error
                            );
                            if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
                                if io_error.kind() == std::io::ErrorKind::BrokenPipe {
                                    info!("SentryClientReactor.EventLoop TODO: need to reconnect in_stream");
                                }
                            }
                        }
                    }
                }
                EventLoopStreamId::Stop => {
                    break;
                }
            }
        }

        info!("SentryClientReactor stopped");
        Ok(())
    }
}
