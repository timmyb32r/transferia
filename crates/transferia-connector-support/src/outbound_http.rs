use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use reqwest::{redirect, Client, Method, RequestBuilder, Response, StatusCode, Url};
use thiserror::Error;

use crate::external_request::elapsed_millis;

#[derive(Debug, Error)]
pub enum OutboundHttpError {
    #[error("outbound HTTP URL was rejected: {reason}")]
    InvalidUrl { reason: &'static str },

    #[error("outbound HTTP redirect was rejected with status {status}")]
    RedirectRejected { status: StatusCode },

    #[error("outbound HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkPolicy {
    PublicInternet,
    AllowPrivateNetworks,
}

#[derive(Debug)]
struct PolicyResolver {
    policy: NetworkPolicy,
}

impl reqwest::dns::Resolve for PolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        let policy = self.policy;
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(format!("DNS returned no addresses for {host}").into());
            }
            if let Some(address) = addresses
                .iter()
                .find(|address| !is_allowed_address(policy, address.ip()))
            {
                return Err(format!(
                    "DNS for {host} resolved to an address forbidden by {policy:?}: {}",
                    address.ip()
                )
                .into());
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[derive(Clone)]
pub struct OutboundHttpClient {
    inner: Client,
    policy: NetworkPolicy,
    pinned_host: Option<&'static str>,
}

impl OutboundHttpClient {
    pub fn new(
        timeout: Duration,
        root_certificates: impl IntoIterator<Item = reqwest::Certificate>,
        policy: NetworkPolicy,
    ) -> Result<Self, reqwest::Error> {
        let mut builder = Client::builder()
            .timeout(timeout)
            .redirect(redirect::Policy::none())
            .dns_resolver(Arc::new(PolicyResolver { policy }));
        for certificate in root_certificates {
            builder = builder.add_root_certificate(certificate);
        }
        Ok(Self {
            inner: builder.build()?,
            policy,
            pinned_host: None,
        })
    }

    /// Build a client whose single allowed hostname connects through a local
    /// reverse tunnel while retaining the original TLS hostname and SNI.
    pub fn new_with_loopback_tunnel(
        timeout: Duration,
        root_certificates: impl IntoIterator<Item = reqwest::Certificate>,
        policy: NetworkPolicy,
        host: &'static str,
        port: u16,
    ) -> Result<Self, reqwest::Error> {
        let mut builder = Client::builder()
            .timeout(timeout)
            .redirect(redirect::Policy::none())
            .dns_resolver(Arc::new(PolicyResolver { policy }))
            .resolve(host, (Ipv4Addr::LOCALHOST, port).into());
        for certificate in root_certificates {
            builder = builder.add_root_certificate(certificate);
        }
        Ok(Self {
            inner: builder.build()?,
            policy,
            pinned_host: Some(host),
        })
    }

    #[must_use]
    pub fn get(&self, url: Url) -> OutboundHttpRequest {
        self.request(Method::GET, url)
    }

    #[must_use]
    pub fn request(&self, method: Method, url: Url) -> OutboundHttpRequest {
        let target = http_target(&url);
        let policy_error = if !matches!(url.scheme(), "http" | "https") {
            Some("scheme must be http or https")
        } else if self
            .pinned_host
            .is_some_and(|host| url.host_str() != Some(host))
        {
            Some("URL host does not match the client's pinned tunnel host")
        } else if !url.username().is_empty() || url.password().is_some() {
            Some("credentials must not be embedded in the URL")
        } else if url
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .is_some_and(|address| !is_allowed_address(self.policy, address))
        {
            Some("network policy forbids this IP address")
        } else {
            None
        };
        OutboundHttpRequest {
            inner: self.inner.request(method.clone(), url),
            policy_error,
            method,
            target,
        }
    }

    /// Clone the policy-configured transport for SDK adapters that accept a
    /// `reqwest::Client` but do not expose their own redirect policy.
    #[must_use]
    pub fn transport(&self) -> Client {
        self.inner.clone()
    }
}

fn is_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_allowed_address(policy: NetworkPolicy, address: IpAddr) -> bool {
    match policy {
        NetworkPolicy::PublicInternet => is_public_address(address),
        NetworkPolicy::AllowPrivateNetworks => !is_special_address(address),
    }
}

fn is_special_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_link_local()
                || address.is_unspecified()
                || address.is_multicast()
                || address == Ipv4Addr::BROADCAST
                || octets[0] == 0
                || octets[0] >= 240
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        }
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return is_special_address(IpAddr::V4(mapped));
            }
            let segments = address.segments();
            address.is_unspecified()
                || address.is_multicast()
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    !(address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_multicast()
        || address == Ipv4Addr::BROADCAST
        || octets[0] == 0
        || octets[0] >= 240
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113))
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    !(address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8))
}

pub struct OutboundHttpRequest {
    inner: RequestBuilder,
    policy_error: Option<&'static str>,
    method: Method,
    target: String,
}

impl OutboundHttpRequest {
    #[must_use]
    pub fn configure(self, configure: impl FnOnce(RequestBuilder) -> RequestBuilder) -> Self {
        Self {
            inner: configure(self.inner),
            policy_error: self.policy_error,
            method: self.method,
            target: self.target,
        }
    }

    pub async fn send(self) -> Result<Response, OutboundHttpError> {
        if let Some(reason) = self.policy_error {
            return Err(OutboundHttpError::InvalidUrl { reason });
        }
        let started = Instant::now();
        let response = match self.inner.send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::info!(
                    target: "transferia.external_request",
                    external_system = "http",
                    operation = %self.method,
                    target = %self.target,
                    elapsed_ms = elapsed_millis(started),
                    outcome = "transport_error",
                    "external request completed"
                );
                return Err(error.into());
            }
        };
        let status = response.status();
        tracing::info!(
            target: "transferia.external_request",
            external_system = "http",
            operation = %self.method,
            target = %self.target,
            elapsed_ms = elapsed_millis(started),
            status = status.as_u16(),
            outcome = if status.is_redirection() { "redirect_rejected" } else { "response" },
            "external request completed"
        );
        if response.status().is_redirection() {
            return Err(OutboundHttpError::RedirectRejected {
                status: response.status(),
            });
        }
        Ok(response)
    }
}

fn http_target(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    url.port().map_or_else(
        || format!("{}://{host}", url.scheme()),
        |port| format!("{}://{host}:{port}", url.scheme()),
    )
}

#[cfg(test)]
mod tests;
