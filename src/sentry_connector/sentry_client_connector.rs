use super::{
    sentry_address::SentryAddress,
    sentry_client::{SentryClient, Status},
    sentry_client_impl::SentryClientImpl,
};
use async_trait::async_trait;
use futures_core::Stream;
use std::pin::Pin;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::*;

#[async_trait]
pub trait SentryClientConnector: Send {
    async fn connect(&mut self, status: Status) -> anyhow::Result<Box<dyn SentryClient>>;
}

pub struct SentryClientConnectorImpl {
    sentry_api_addr: SentryAddress,
}

impl SentryClientConnectorImpl {
    pub fn new(sentry_api_addr: SentryAddress) -> Self {
        Self { sentry_api_addr }
    }
}

const RECONNECT_TIMEOUT: u64 = 5; // seconds

#[async_trait]
impl SentryClientConnector for SentryClientConnectorImpl {
    async fn connect(&mut self, status: Status) -> anyhow::Result<Box<dyn SentryClient>> {
        loop {
            let sentry_api_addr = self.sentry_api_addr.clone();
            let result = SentryClientImpl::new(sentry_api_addr).await;
            match result {
                Ok(mut client) => {
                    let status_result = client.set_status(status.clone()).await;
                    match status_result {
                        Ok(_) => return Ok(Box::new(client)),
                        Err(error) => {
                            if is_disconnect_error(&error) {
                                error!("Sentry client disconnected during set_status");
                                time::sleep(time::Duration::from_secs(RECONNECT_TIMEOUT)).await;
                                continue;
                            }
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    if is_tonic_transport_error(&error) {
                        let sentry_api_addr = self.sentry_api_addr.clone();
                        error!("Sentry server is unreachable at {:?}", sentry_api_addr);
                        time::sleep(time::Duration::from_secs(RECONNECT_TIMEOUT)).await;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }
}

pub type SentryClientConnectorStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<Box<dyn SentryClient>>> + Send>>;

pub type StatusStream = Pin<Box<dyn Stream<Item = anyhow::Result<Status>> + Send>>;

pub fn make_connector_stream(
    mut connector: Box<dyn SentryClientConnector>,
    mut current_status_stream: StatusStream,
) -> SentryClientConnectorStream {
    Box::pin(async_stream::stream! {
        loop {
            let status = current_status_stream.next().await
                .unwrap_or(Err(anyhow::format_err!("SentryClientConnectorStream failed to retrieve the current Status.")))?;
            let client = connector.connect(status).await?;
            yield Ok(client);
        }
    })
}

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

pub struct SentryClientConnectorTest {
    test_client: Option<Box<dyn SentryClient>>,
}

impl SentryClientConnectorTest {
    pub fn new(test_client: Box<dyn SentryClient>) -> Self {
        Self {
            test_client: Some(test_client),
        }
    }
}

#[async_trait]
impl SentryClientConnector for SentryClientConnectorTest {
    async fn connect(&mut self, status: Status) -> anyhow::Result<Box<dyn SentryClient>> {
        if let Some(mut test_client) = self.test_client.take() {
            test_client.set_status(status).await?;
            Ok(test_client)
        } else {
            Err(anyhow::format_err!(
                "SentryClientConnectorTest doesn't support reconnects."
            ))
        }
    }
}
