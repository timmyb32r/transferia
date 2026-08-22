use std::fmt;
use std::time::Duration;

use object_store::client::{HttpClient, HttpConnector};
use transferia_connector_support::outbound_http::OutboundHttpClient;

#[derive(Clone)]
pub(super) struct NoRedirectConnector {
    client: reqwest::Client,
}

impl NoRedirectConnector {
    pub(super) fn new(timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            client: OutboundHttpClient::new(timeout, [])?.transport(),
        })
    }
}

impl fmt::Debug for NoRedirectConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("NoRedirectConnector").finish()
    }
}

impl HttpConnector for NoRedirectConnector {
    fn connect(&self, _options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.client.clone()))
    }
}
