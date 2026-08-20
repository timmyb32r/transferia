use std::time::Duration;

use reqwest::{redirect, Client, Method, RequestBuilder, Response, StatusCode, Url};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutboundHttpError {
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

    pub fn get(&self, url: Url) -> OutboundHttpRequest {
        self.request(Method::GET, url)
    }

    pub fn request(&self, method: Method, url: Url) -> OutboundHttpRequest {
        OutboundHttpRequest {
            inner: self.inner.request(method, url),
        }
    }
}

pub struct OutboundHttpRequest {
    inner: RequestBuilder,
}

impl OutboundHttpRequest {
    pub fn configure(self, configure: impl FnOnce(RequestBuilder) -> RequestBuilder) -> Self {
        Self {
            inner: configure(self.inner),
        }
    }

    pub async fn send(self) -> Result<Response, OutboundHttpError> {
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
