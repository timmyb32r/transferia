use std::time::Duration;

use reqwest::{redirect, Client, Method, RequestBuilder, Response, StatusCode, Url};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutboundHttpError {
    #[error("outbound HTTP URL was rejected: {reason}")]
    InvalidUrl { reason: &'static str },

    #[error("outbound HTTP redirect was rejected with status {status}")]
    RedirectRejected { status: StatusCode },

    #[error("outbound HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
}

#[derive(Clone)]
pub struct OutboundHttpClient {
    inner: Client,
}

impl OutboundHttpClient {
    pub fn new(
        timeout: Duration,
        root_certificates: impl IntoIterator<Item = reqwest::Certificate>,
    ) -> Result<Self, reqwest::Error> {
        let mut builder = Client::builder()
            .timeout(timeout)
            .redirect(redirect::Policy::none());
        for certificate in root_certificates {
            builder = builder.add_root_certificate(certificate);
        }
        Ok(Self {
            inner: builder.build()?,
        })
    }

    #[must_use]
    pub fn get(&self, url: Url) -> OutboundHttpRequest {
        self.request(Method::GET, url)
    }

    #[must_use]
    pub fn request(&self, method: Method, url: Url) -> OutboundHttpRequest {
        let policy_error = if !matches!(url.scheme(), "http" | "https") {
            Some("scheme must be http or https")
        } else if !url.username().is_empty() || url.password().is_some() {
            Some("credentials must not be embedded in the URL")
        } else {
            None
        };
        OutboundHttpRequest {
            inner: self.inner.request(method, url),
            policy_error,
        }
    }

    /// Clone the policy-configured transport for SDK adapters that accept a
    /// `reqwest::Client` but do not expose their own redirect policy.
    #[must_use]
    pub fn transport(&self) -> Client {
        self.inner.clone()
    }
}

pub struct OutboundHttpRequest {
    inner: RequestBuilder,
    policy_error: Option<&'static str>,
}

impl OutboundHttpRequest {
    #[must_use]
    pub fn configure(self, configure: impl FnOnce(RequestBuilder) -> RequestBuilder) -> Self {
        Self {
            inner: configure(self.inner),
            policy_error: self.policy_error,
        }
    }

    pub async fn send(self) -> Result<Response, OutboundHttpError> {
        if let Some(reason) = self.policy_error {
            return Err(OutboundHttpError::InvalidUrl { reason });
        }
        let response = self.inner.send().await?;
        if response.status().is_redirection() {
            return Err(OutboundHttpError::RedirectRejected {
                status: response.status(),
            });
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests;
