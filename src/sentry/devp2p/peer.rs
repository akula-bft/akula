use super::{ecies::ECIESStream, transport::Transport, types::*, util::pk2id};
use anyhow::{anyhow, bail, Context as _};
use bytes::{Bytes, BytesMut};
use derive_more::Display;
use enum_primitive_derive::Primitive;
use fastrlp::*;
use futures::{Sink, SinkExt};
use num_traits::*;
use secp256k1::{PublicKey, SecretKey, SECP256K1};
use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::{Stream, StreamExt};
use tracing::*;

const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// RLPx disconnect reason.
#[derive(Clone, Copy, Debug, Display, Primitive)]
pub enum DisconnectReason {
    #[display(fmt = "disconnect requested")]
    DisconnectRequested = 0x00,
    #[display(fmt = "TCP sub-system error")]
    TcpSubsystemError = 0x01,
    #[display(fmt = "breach of protocol, e.g. a malformed message, bad RLP, ...")]
    ProtocolBreach = 0x02,
    #[display(fmt = "useless peer")]
    UselessPeer = 0x03,
    #[display(fmt = "too many peers")]
    TooManyPeers = 0x04,
    #[display(fmt = "already connected")]
    AlreadyConnected = 0x05,
    #[display(fmt = "incompatible P2P protocol version")]
    IncompatibleP2PProtocolVersion = 0x06,
    #[display(fmt = "null node identity received - this is automatically invalid")]
    NullNodeIdentity = 0x07,
    #[display(fmt = "client quitting")]
    ClientQuitting = 0x08,
    #[display(fmt = "unexpected identity in handshake")]
    UnexpectedHandshakeIdentity = 0x09,
    #[display(fmt = "identity is the same as this node (i.e. connected to itself)")]
    ConnectedToSelf = 0x0a,
    #[display(fmt = "ping timeout")]
    PingTimeout = 0x0b,
    #[display(fmt = "some other reason specific to a subprotocol")]
    SubprotocolSpecific = 0x10,
}

/// RLPx protocol version.
#[derive(Copy, Clone, Debug, Primitive)]
pub enum ProtocolVersion {
    V5 = 5,
}

#[derive(Clone, Debug, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct CapabilityMessage {
    pub name: CapabilityName,
    pub version: usize,
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct HelloMessage {
    pub protocol_version: usize,
    pub client_version: String,
    pub capabilities: Vec<CapabilityMessage>,
    pub port: u16,
    pub id: PeerId,
}

#[derive(Debug)]
struct Snappy {
    encoder: snap::raw::Encoder,
    decoder: snap::raw::Decoder,
}

impl Default for Snappy {
    fn default() -> Self {
        Self {
            encoder: snap::raw::Encoder::new(),
            decoder: snap::raw::Decoder::new(),
        }
    }
}

/// RLPx transport peer stream
#[allow(unused)]
#[derive(Debug)]
pub struct PeerStream<Io> {
    stream: ECIESStream<Io>,
    client_version: String,
    shared_capabilities: Vec<CapabilityInfo>,
    port: u16,
    id: PeerId,
    remote_id: PeerId,

    snappy: Snappy,

    disconnected: bool,
}

impl<Io> PeerStream<Io>
where
    Io: Transport,
{
    /// Remote public id of this peer
    pub fn remote_id(&self) -> PeerId {
        self.remote_id
    }

    /// Get all capabilities of this peer stream
    pub fn capabilities(&self) -> &[CapabilityInfo] {
        &self.shared_capabilities
    }

    /// Connect to a peer over TCP
    pub async fn connect(
        transport: Io,
        secret_key: SecretKey,
        remote_id: PeerId,
        client_version: String,
        capabilities: Vec<CapabilityInfo>,
        port: u16,
    ) -> anyhow::Result<Self> {
        Self::new(
            ECIESStream::connect(transport, secret_key, remote_id).await?,
            secret_key,
            client_version,
            capabilities,
            port,
        )
        .await
    }

    /// Incoming peer stream over TCP
    pub async fn incoming(
        transport: Io,
        secret_key: SecretKey,
        client_version: String,
        capabilities: Vec<CapabilityInfo>,
        port: u16,
    ) -> anyhow::Result<Self> {
        Self::new(
            ECIESStream::incoming(transport, secret_key).await?,
            secret_key,
            client_version,
            capabilities,
            port,
        )
        .await
    }

    /// Create a new peer stream
    #[instrument(skip_all, fields(id=&*transport.remote_id().to_string()))]
    pub async fn new(
        mut transport: ECIESStream<Io>,
        secret_key: SecretKey,
        client_version: String,
        capabilities: Vec<CapabilityInfo>,
        port: u16,
    ) -> anyhow::Result<Self> {
        let public_key = PublicKey::from_secret_key(SECP256K1, &secret_key);
        let id = pk2id(&public_key);
        let nonhello_capabilities = capabilities.clone();
        let nonhello_client_version = client_version.clone();

        debug!("Connecting to RLPx peer {:02x}", transport.remote_id());

        let hello = HelloMessage {
            port,
            id,
            protocol_version: ProtocolVersion::V5.to_usize().unwrap(),
            client_version,
            capabilities: {
                let mut caps = Vec::new();
                for cap in &capabilities {
                    caps.push(CapabilityMessage {
                        name: cap.name.clone(),
                        version: cap.version,
                    });
                }
                caps
            },
        };
        trace!("Sending hello message: {:?}", hello);

        let mut outbound_hello = BytesMut::new();
        0_u8.encode(&mut outbound_hello);
        hello.encode(&mut outbound_hello);

        trace!("Outbound hello: {}", hex::encode(&outbound_hello));
        transport.send(outbound_hello.freeze()).await?;

        let hello = transport.try_next().await?;

        let hello = hello.ok_or_else(|| {
            debug!("Hello failed because of no value");
            anyhow!("hello failed (no value)")
        })?;
        trace!("Receiving hello message: {:02x?}", hello);

        let message_id = u8::decode(&mut &hello[..1])?;
        let payload = &mut &hello[1..];
        match message_id {
            0 => {}
            1 => {
                let reason = u8::decode(payload).map(DisconnectReason::from_u8)?;
                bail!(
                    "explicit disconnect: {}",
                    reason
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "(unknown)".to_string())
                );
            }
            _ => {
                bail!(
                    "Hello failed because message id is not 0 but {}: {:02x?}",
                    message_id,
                    payload
                );
            }
        }

        let val = HelloMessage::decode(payload).context("hello failed (rlp)")?;
        debug!("hello message: {:?}", val);
        let mut shared_capabilities: Vec<CapabilityInfo> = Vec::new();

        for cap_info in nonhello_capabilities {
            let cap_match = val
                .capabilities
                .iter()
                .any(|v| v.name == cap_info.name && v.version == cap_info.version);

            if cap_match {
                shared_capabilities.push(cap_info);
            }
        }

        let shared_caps_original = shared_capabilities.clone();

        for cap_info in shared_caps_original {
            shared_capabilities
                .retain(|v| v.name != cap_info.name || v.version >= cap_info.version);
        }

        shared_capabilities.sort_by_cached_key(|v| v.name.clone());

        let no_shared_caps = shared_capabilities.is_empty();

        let mut this = Self {
            remote_id: transport.remote_id(),
            stream: transport,
            client_version: nonhello_client_version,
            port,
            id,
            shared_capabilities,
            snappy: Snappy::default(),
            disconnected: false,
        };

        if no_shared_caps {
            debug!("No shared capabilities, disconnecting.");
            let _ = this
                .send(PeerMessage::Disconnect(DisconnectReason::UselessPeer))
                .await;

            bail!(
                "Handshake failed - no shared capabilities (our: {:?}, their: {:?})",
                capabilities,
                val.capabilities
            );
        }

        Ok(this)
    }
}

/// Sending message for RLPx
#[derive(Clone, Debug)]
pub struct SubprotocolMessage {
    pub cap_name: CapabilityName,
    pub message: Message,
}

#[derive(Clone, Debug)]
pub enum PeerMessage {
    Disconnect(DisconnectReason),
    Ping,
    Pong,
    Subprotocol(SubprotocolMessage),
}

impl<Io> Stream for PeerStream<Io>
where
    Io: Transport,
{
    type Item = Result<PeerMessage, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut s = self.get_mut();

        if s.disconnected {
            return Poll::Ready(None);
        }

        match Pin::new(&mut s.stream).poll_next(cx).ready()? {
            Some(Ok(val)) => {
                trace!("Received peer message: {}", hex::encode(&val));

                let (cap, id, data) = match u8::decode(&mut &val[..1]) {
                    Ok(message_id) => {
                        let input = &val[1..];
                        let payload_len = snap::raw::decompress_len(input)?;
                        if payload_len > MAX_PAYLOAD_SIZE {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "payload size ({}) exceeds limit ({} bytes)",
                                    payload_len, MAX_PAYLOAD_SIZE
                                ),
                            ))));
                        }
                        let data = Bytes::from(s.snappy.decoder.decompress_vec(input)?);
                        trace!("Decompressed raw message data: {}", hex::encode(&data));

                        if message_id < 0x10 {
                            match message_id {
                                0x01 => {
                                    s.disconnected = true;
                                    if let Some(reason) = u8::decode(&mut &*data)
                                        .ok()
                                        .and_then(DisconnectReason::from_u8)
                                    {
                                        return Poll::Ready(Some(Ok(PeerMessage::Disconnect(
                                            reason,
                                        ))));
                                    } else {
                                        return Poll::Ready(Some(Err(io::Error::new(
                                            io::ErrorKind::Other,
                                            format!(
                                                "peer disconnected with malformed message: {}",
                                                hex::encode(data)
                                            ),
                                        ))));
                                    }
                                }
                                0x02 => {
                                    debug!("received ping message data {:?}", data);
                                    return Poll::Ready(Some(Ok(PeerMessage::Ping)));
                                }
                                0x03 => {
                                    debug!("received pong message");
                                    return Poll::Ready(Some(Ok(PeerMessage::Pong)));
                                }
                                _ => {
                                    debug!("received unknown reserved message");
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        "unhandled reserved message",
                                    ))));
                                }
                            }
                        }

                        let mut message_id = message_id as usize - 0x10;
                        let mut index = 0;
                        for cap in &s.shared_capabilities {
                            if message_id > cap.length {
                                message_id -= cap.length;
                                index += 1;
                            }
                        }
                        if index >= s.shared_capabilities.len() {
                            return Poll::Ready(Some(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "invalid message id (out of cap range)",
                            ))));
                        }
                        (s.shared_capabilities[index].clone(), message_id, data)
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("message id parsing failed (invalid): {}", e),
                        ))));
                    }
                };

                trace!(
                    "Cap: {}, id: {}, data: {}",
                    CapabilityId::from(cap.clone()),
                    id,
                    hex::encode(&data)
                );

                Poll::Ready(Some(Ok(PeerMessage::Subprotocol(SubprotocolMessage {
                    cap_name: cap.name.clone(),
                    message: Message { id, data },
                }))))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl<Io> Sink<PeerMessage> for PeerStream<Io>
where
    Io: Transport,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, message: PeerMessage) -> Result<(), Self::Error> {
        let this = self.get_mut();

        if this.disconnected {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "disconnection requested",
            ));
        }

        let (message_id, payload) = match message {
            PeerMessage::Disconnect(reason) => {
                this.disconnected = true;
                (
                    0x01,
                    fastrlp::encode_fixed_size(&reason.to_u8().unwrap())
                        .to_vec()
                        .into(),
                )
            }
            PeerMessage::Ping => {
                debug!("sending ping message");
                (0x02, Bytes::from_static(&[EMPTY_LIST_CODE]))
            }
            PeerMessage::Pong => {
                debug!("sending pong message");
                (0x03, Bytes::from_static(&[EMPTY_LIST_CODE]))
            }
            PeerMessage::Subprotocol(SubprotocolMessage { cap_name, message }) => {
                let Message { id, data } = message;
                let cap = this
                    .shared_capabilities
                    .iter()
                    .find(|cap| cap.name == cap_name)
                    .unwrap_or_else(|| {
                        panic!(
                            "attempted to send payload of unsupported capability ({}/{}/{})",
                            cap_name.0,
                            id,
                            this.remote_id(),
                        )
                    });

                assert!(
                    id < cap.length,
                    "attempted to send payload with message id too big ({}/{}/{})",
                    cap_name.0,
                    id,
                    this.remote_id()
                );

                let mut message_id = 0x10;
                for scap in &this.shared_capabilities {
                    if scap == cap {
                        break;
                    }

                    message_id += scap.length;
                }
                message_id += id;

                (message_id, data)
            }
        };

        let mut msg = BytesMut::with_capacity(2 + payload.len());
        message_id.encode(&mut msg);

        let mut buf = msg.split_off(msg.len());

        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "payload size ({}) exceeds limit ({} bytes)",
                    payload.len(),
                    MAX_PAYLOAD_SIZE
                ),
            ));
        }

        buf.resize(snap::raw::max_compress_len(payload.len()), 0);

        let compressed_len = this.snappy.encoder.compress(&payload, &mut buf).unwrap();
        buf.truncate(compressed_len);

        msg.unsplit(buf);

        Pin::new(&mut this.stream).start_send(msg.freeze())?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use fastrlp::Decodable;

    use super::HelloMessage;

    #[test]
    fn test_hello_message() {
        let test_cases = vec![
            b"\x80\xf8\xc0\x05\xb8>Geth/*****/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17.2\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@exx\xa6M\x86\xe0\xff\xady\x9cI\xbb\x9f\xc8\xcfY\x81kbb\xe6v\xf0q\x90p\xfda0EQ\xcf\x01\x8f)J\xc1\xb3\xce\x87\x1a+\xe1{\xff\x0f\x16g\xea*ct\x83\x16\xdeY\xe3\xa4\xd3H\x0f\xca\x8c".to_vec(),
            b"\x80\xf8\xc7\x05\xb8MGeth/**********/v1.9.25-stable-919800f0(quorum-v21.10.0)/linux-amd64/go1.15.6\xf3\xc5\x83eth?\xc5\x83eth@\xc5\x83ethA\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\x80\xb8@3i\0L\xcd \xf4\xfdW\xe3\x1e\xe7\xceY\x14\x9a\xaf\xa8\x8a\xf3\x13&\x01[\x0fa&M\xdbn\x129\x11]\xdd\0\x97\x97%l|\xae\x08\xf8\xfc\r1\x9d8\xb2\x1826\xa0\x04\xe7qZO]\xc9B\xee\xa1".to_vec(),
            b"\x80\xf8\xc8\x05\xb8NGeth/***********/v1.9.25-stable-919800f0(quorum-v21.10.0)/linux-amd64/go1.15.6\xf3\xc5\x83eth?\xc5\x83eth@\xc5\x83ethA\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\x80\xb8@\xcdy\xef]*\xfa\xa5\x91\x81\xf3`\xd9~\xa7\xb9>\xb9\x80\xee\xe0\xb3\xe1\xd4(\xc1\xfc.\xe41\xc9\xe7I\xac\xa1B\xafp\xfc;\xce\xae(\r\x11\x89\x07_\xb0|\xa0z\0\xf5{1`\xe3\xdb\xff\x1c\xf8\x97\xa2\x81".to_vec(),
            b"\x80\xf8\xc4\x05\xb8BGeth/***********/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@\xe0\xacc\xf1\xaeD#\xf5\x8b\tB\x91\xd2I\xe4\x88>&\x07\xa9\x01s\xf9\xa1Hk,{\x88;:\xc9\xbcn\xd3bE3{\x94^\xfa\xd15\xb0\xf6\xcb!\n\x1e\xb1\x13L\x1aq}L\x99W\x88;\xe2\x0f\xde".to_vec(),
            b"\x80\xf8\xc5\x05\xb8CGeth/**********/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17.5\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@\xeb\x9b\x81\x04I\xe8\xb0\xd8Ut\xb1\x1d\xe3\x14U)\xcb\xd7PB\xba\xd5\xdeG\xcb.\xdfD\x93q\xb3'5\xdd\xdf\xa2i\xfc\x8f\x84\xb4\xc3\xfc\n\xad\r\x81(1\x91l.\xf2\xcfI\x03\xa9\x0cc\x8f\xc0P\x80A".to_vec(),
            b"\x80\xf8\xc3\x05\xb8AGeth/********/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17.5\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@M\x16a\xf6\xec\x123\x86\xe1\x19+\xf4\xc5c$\x8fGIw\xe4\x08{\x95jf\x9a\x92\x7f\x1f\x12B\x19$\x7f\xf35\xa5\x18%L\x844\x06\x11H\x94\xd2\x95\x8a\xc2zD3f\x96qJ7\x0c\xe5I\x17\x10e".to_vec(),
            b"\x80\xf8\xbf\x05\xb8=Geth/****/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17.5\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@\xec\xe42\x98!\xbc\xd6C\xd0\x86\xc2_\xbe\xf3\xdb\xb2\xd6\x94$\x87\x11Cw\xd0\xc1\x9e\xa6\xfd\x1a\xdc\x99\x9eX\xba%\x9e\xe3\xc1]\x1b\xee\xf9D\x02\x94\x18\x9a\n.\xc8\xdb\x19\x1fQ\xc6\xfeE\x14\x8c\xcd\xd7\xeem8".to_vec(),
            b"\x80\xf8\xc1\x05\xb8?Geth/******/v1.10.0-stable(quorum-v22.1.0)/linux-amd64/go1.17.5\xf8:\xc5\x83eth@\xc5\x83ethA\xc5\x83ethB\xca\x88istanbul@\xca\x88istanbulc\xca\x88istanbuld\xc6\x84snap\x01\x80\xb8@\x87\xa9\xad\xe9WI!s\xd2r[\x93\xa7p\xf5/\xa9\xeb7\x9c\xeb\xb1\x9fjmLCI\xab'\x12>\xa3)~\x0f\x91\x94\xb1\x87h~\x7f\x12*\xb5\x9f\x1f\x83Y\xd3\x8d\x93\xbfoG`\xda\n#\xb0>u\x96".to_vec(),
        ];

        for msg in &test_cases {
            let mut payload = &msg[1..];
            HelloMessage::decode(&mut payload).expect("error");
        }
    }
}
