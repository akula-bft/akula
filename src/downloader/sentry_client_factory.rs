use crate::downloader::{
    message_decoder::decode_rlp_message,
    sentry_address::SentryAddress,
    sentry_client::SentryClient,
    sentry_client_impl::{MessageDecoder, SentryClientImpl},
};

pub async fn create(addr: SentryAddress) -> anyhow::Result<Box<dyn SentryClient>> {
    let decoder: &'static MessageDecoder = &(decode_rlp_message as MessageDecoder);
    Ok(Box::new(SentryClientImpl::new(addr, decoder).await?))
}
