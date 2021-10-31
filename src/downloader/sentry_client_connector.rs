use crate::downloader::{
    sentry_address::SentryAddress, sentry_client::SentryClient,
    sentry_client_impl::SentryClientImpl,
};
use futures_core::Stream;
use std::pin::Pin;
use tracing::*;

fn is_tonic_transport_error(error: &anyhow::Error) -> bool {
    if let Some(transport_error) = error.downcast_ref::<tonic::transport::Error>() {
        let transport_error_str = format!("{}", transport_error);
        return transport_error_str == "transport error";
    }
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return (status.code() == tonic::Code::Unknown) && (status.message() == "transport error");
    }
    false
}

pub async fn connect(sentry_api_addr: SentryAddress) -> anyhow::Result<Box<dyn SentryClient>> {
    loop {
        let result = SentryClientImpl::new(sentry_api_addr.clone()).await;
        match result {
            Ok(client) => return Ok(Box::new(client)),
            Err(error) => {
                if is_tonic_transport_error(&error) {
                    error!("Sentry server is unreachable at {:?}", sentry_api_addr);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

pub type SentryClientConnectorStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<Box<dyn SentryClient>>> + Send>>;

pub fn make_connector_stream(
    sentry_client: Box<dyn SentryClient>,
    sentry_api_addr: SentryAddress,
) -> SentryClientConnectorStream {
    Box::pin(async_stream::stream! {
        yield Ok(sentry_client);
        loop {
            let client = connect(sentry_api_addr.clone()).await?;
            yield Ok(client);
        }
    })
}

pub fn is_disconnect_error(error: &anyhow::Error) -> bool {
    if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
        if io_error.kind() == std::io::ErrorKind::BrokenPipe {
            return true;
        }
    }
    if is_tonic_transport_error(error) {
        return true;
    }
    false
}
