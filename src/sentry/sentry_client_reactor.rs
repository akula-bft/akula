use super::{
    messages::{EthMessageId, Message},
    sentry_client::*,
    sentry_client_connector,
};
use futures_core::{Future, Stream};
use futures_util::{FutureExt, TryStreamExt};
use parking_lot::RwLock;
use std::{collections::HashMap, fmt, pin::Pin, sync::Arc};
use strum::IntoEnumIterator;
use tokio::{
    sync::{broadcast, mpsc, Mutex},
    task::JoinHandle,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, ReceiverStream},
    StreamExt, StreamMap,
};
use tracing::*;

pub type SentryClientReactorShared = Arc<tokio::sync::RwLock<SentryClientReactor>>;

type ReceiveMessagesSenders =
    Arc<RwLock<HashMap<EthMessageId, broadcast::Sender<MessageFromPeer>>>>;

pub struct SentryClientReactor {
    send_message_sender: mpsc::Sender<SentryCommand>,
    receive_messages_senders: ReceiveMessagesSenders,
    event_loop: Mutex<Option<SentryClientReactorEventLoop>>,
    event_loop_handle: Option<JoinHandle<()>>,
    stop_signal_sender: mpsc::Sender<()>,
}

struct SentryClientReactorEventLoop {
    sentry_connector: sentry_client_connector::SentryClientConnectorStream,
    send_message_receiver: mpsc::Receiver<SentryCommand>,
    receive_messages_senders: ReceiveMessagesSenders,
    stop_signal_receiver: mpsc::Receiver<()>,
}

#[derive(Clone, Debug)]
enum SentryCommand {
    SendMessage(SendMessageParams),
    PenalizePeer(PeerId),
}

#[derive(Clone, Debug)]
struct SendMessageParams {
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
    pub fn new(sentry_connector: sentry_client_connector::SentryClientConnectorStream) -> Self {
        let (send_message_sender, send_message_receiver) = mpsc::channel::<SentryCommand>(1);

        let mut receive_messages_senders =
            HashMap::<EthMessageId, broadcast::Sender<MessageFromPeer>>::new();
        for id in EthMessageId::iter() {
            let (sender, _) = broadcast::channel::<MessageFromPeer>(1024);
            receive_messages_senders.insert(id, sender);
        }
        let receive_messages_senders = Arc::new(RwLock::new(receive_messages_senders));

        let (stop_signal_sender, stop_signal_receiver) = mpsc::channel::<()>(1);

        let event_loop = SentryClientReactorEventLoop {
            sentry_connector,
            send_message_receiver,
            receive_messages_senders: Arc::clone(&receive_messages_senders),
            stop_signal_receiver,
        };

        Self {
            send_message_sender,
            receive_messages_senders: Arc::clone(&receive_messages_senders),
            event_loop: Mutex::new(Some(event_loop)),
            event_loop_handle: None,
            stop_signal_sender,
        }
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let event_loop = self
            .event_loop
            .try_lock()?
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

    pub async fn penalize_peer(&self, peer_id: PeerId) -> anyhow::Result<()> {
        let command = SentryCommand::PenalizePeer(peer_id);
        let result = self.send_message_sender.send(command).await;
        result.map_err(|_| anyhow::Error::new(SendMessageError::ReactorStopped))
    }

    pub async fn send_message(
        &self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        let params = SendMessageParams {
            message,
            peer_filter,
        };
        let command = SentryCommand::SendMessage(params);
        let result = self.send_message_sender.send(command).await;
        result.map_err(|_| anyhow::Error::new(SendMessageError::ReactorStopped))
    }

    pub fn try_send_message(
        &self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        let params = SendMessageParams {
            message,
            peer_filter,
        };
        let command = SentryCommand::SendMessage(params);
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
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        let sender = self.send_message_sender.clone();
        Box::pin(sender.reserve_owned().map(|result| match result {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow::Error::new(SendMessageError::ReactorStopped)),
        }))
    }

    pub fn receive_messages(
        &self,
        filter_id: EthMessageId,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = MessageFromPeer> + Send>>> {
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
    Sentry,
    Send,
    Receive,
    Stop,
}

#[derive(Debug)]
enum EventLoopStreamResult {
    Sentry(Box<dyn SentryClient>),
    Send(u32),
    Receive(MessageFromPeer),
    Stop(()),
}

type EventLoopStream = Pin<Box<dyn Stream<Item = anyhow::Result<EventLoopStreamResult>> + Send>>;

// dropping this causes dropping the senders and triggers an end of stream event on the receivers end
#[derive(Clone)]
struct EventLoopReceiveMessagesSendersDropper {
    receive_messages_senders: ReceiveMessagesSenders,
    is_auto: bool,
}

impl EventLoopReceiveMessagesSendersDropper {
    pub fn disable_auto_drop(&mut self) {
        self.is_auto = false;
    }

    pub fn force_drop(&self) {
        self.receive_messages_senders.write().clear();
    }
}

impl Drop for EventLoopReceiveMessagesSendersDropper {
    fn drop(&mut self) {
        // drop shared senders so that existing receive_messages() streams end
        if self.is_auto {
            self.force_drop();
        }
    }
}

mod stream_factory {
    use super::super::{sentry_client::MessageFromPeerStream, sentry_client_reactor::*};
    use tokio::sync::Mutex;

    fn make_receive_stream(
        mut stream: MessageFromPeerStream,
        mut receive_messages_senders_dropper: EventLoopReceiveMessagesSendersDropper,
    ) -> EventLoopStream {
        receive_messages_senders_dropper.disable_auto_drop();

        let receive_stream = async_stream::stream! {
            while let Some(message_result) = stream.next().await {
                yield message_result;
            }

            debug!("SentryClientReactor.EventLoop receive_messages stream ended");

            // When the stream ends (this normally happens during tests)
            // we need to ensure unblocking the subscribers which called receive_messages().
            receive_messages_senders_dropper.force_drop();
        };
        Box::pin(receive_stream.map_ok(EventLoopStreamResult::Receive))
    }

    async fn send_sentry_command(
        command: SentryCommand,
        sentry: &mut Box<dyn SentryClient>,
    ) -> anyhow::Result<u32> {
        match command {
            SentryCommand::SendMessage(params) => {
                sentry
                    .send_message(params.message, params.peer_filter)
                    .await
            }
            SentryCommand::PenalizePeer(peer_id) => {
                // this is sent to a single peer (1)
                sentry.penalize_peer(peer_id).await.map(|_| 1)
            }
        }
    }

    fn make_send_stream(
        send_message_receiver: Arc<Mutex<mpsc::Receiver<SentryCommand>>>,
        mut sentry: Box<dyn SentryClient>,
    ) -> EventLoopStream {
        let send_stream = async_stream::stream! {
            let receiver_lock_result = send_message_receiver.try_lock();
            if receiver_lock_result.is_err() {
                error!("SentryClientReactor.EventLoop send_stream failed to access send_message_receiver");
                assert!(false);
                return;
            }

            let mut receiver = receiver_lock_result.ok().unwrap();
            while let Some(command) = receiver.recv().await {
                let send_result = send_sentry_command(command, &mut sentry).await;
                yield send_result;
            }
        };
        Box::pin(send_stream.map_ok(EventLoopStreamResult::Send))
    }

    pub(super) async fn make_sentry_streams(
        mut sentry: Box<dyn SentryClient>,
        send_message_receiver: Arc<Mutex<mpsc::Receiver<SentryCommand>>>,
        receive_messages_senders_dropper: EventLoopReceiveMessagesSendersDropper,
    ) -> anyhow::Result<(EventLoopStream, EventLoopStream)> {
        // subscribe to incoming messages
        let stream = sentry.receive_messages(&[]).await?;
        let receive_stream = make_receive_stream(stream, receive_messages_senders_dropper);

        let send_stream = make_send_stream(send_message_receiver, sentry);
        Ok((send_stream, receive_stream))
    }
}

impl SentryClientReactorEventLoop {
    async fn run(self) -> anyhow::Result<()> {
        // When the reactor loop stops/aborts (e.g. after calling stop())
        // we need to ensure unblocking the subscribers which called receive_messages().
        let receive_messages_senders_dropper = EventLoopReceiveMessagesSendersDropper {
            receive_messages_senders: Arc::clone(&self.receive_messages_senders),
            is_auto: true,
        };

        let send_message_receiver = Arc::new(Mutex::new(self.send_message_receiver));

        let stop_stream = Box::pin(
            ReceiverStream::new(self.stop_signal_receiver)
                .map(|result| Ok(EventLoopStreamResult::Stop(result))),
        );

        let sentry_stream = Box::pin(self.sentry_connector.map_ok(EventLoopStreamResult::Sentry));
        let mut sentry_stream_holder = Option::<EventLoopStream>::None;

        let mut stream = StreamMap::<EventLoopStreamId, EventLoopStream>::new();
        stream.insert(EventLoopStreamId::Sentry, sentry_stream);
        stream.insert(EventLoopStreamId::Stop, stop_stream);

        while let Some((stream_id, result)) = stream.next().await {
            match stream_id {
                EventLoopStreamId::Sentry => match result {
                    Ok(EventLoopStreamResult::Sentry(sentry)) => {
                        // we have a working sentry, put on hold waiting for more sentries
                        sentry_stream_holder =
                            Some(stream.remove(&EventLoopStreamId::Sentry).unwrap());

                        let (send_stream, receive_stream) = stream_factory::make_sentry_streams(
                            sentry,
                            send_message_receiver.clone(),
                            receive_messages_senders_dropper.clone(),
                        )
                        .await?;

                        stream.insert(EventLoopStreamId::Send, send_stream);
                        stream.insert(EventLoopStreamId::Receive, receive_stream);
                    }
                    Ok(_) => panic!("unexpected result {:?}", result),
                    Err(error) => {
                        error!(
                            "SentryClientReactor.EventLoop sentry connector error: {}",
                            error
                        );
                    }
                },
                EventLoopStreamId::Send => {
                    // process an outgoing message that has been just sent
                    match result {
                        Ok(EventLoopStreamResult::Send(sent_peers_count)) => {
                            debug!(
                                "SentryClientReactor.EventLoop sent command to {:?} peers",
                                sent_peers_count
                            );
                        }
                        Ok(_) => panic!("unexpected result {:?}", result),
                        Err(error) => {
                            error!(
                                "SentryClientReactor.EventLoop send_sentry_command error: {}",
                                error
                            );
                            if sentry_client_connector::is_disconnect_error(&error) {
                                info!("SentryClientReactor.EventLoop reconnecting sentry streams");
                                stream.remove(&EventLoopStreamId::Send);
                                stream.remove(&EventLoopStreamId::Receive);
                                match sentry_stream_holder.take() {
                                    Some(sentry_stream) => {
                                        stream.insert(EventLoopStreamId::Sentry, sentry_stream);
                                    }
                                    None => {
                                        error!("SentryClientReactor.EventLoop unexpected sentry_stream_holder - None");
                                    }
                                }
                            }
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

                            let send_sub_result = sender.send(message_from_peer);
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
                            if sentry_client_connector::is_disconnect_error(&error) {
                                info!("SentryClientReactor.EventLoop reconnecting sentry streams");
                                stream.remove(&EventLoopStreamId::Send);
                                stream.remove(&EventLoopStreamId::Receive);
                                match sentry_stream_holder.take() {
                                    Some(sentry_stream) => {
                                        stream.insert(EventLoopStreamId::Sentry, sentry_stream);
                                    }
                                    None => {
                                        error!("SentryClientReactor.EventLoop unexpected sentry_stream_holder - None");
                                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    struct SyncTester<T: Sync>(T);

    fn assert_sync(sentry: SentryClientReactor) {
        let _ = SyncTester(sentry);
    }
}
