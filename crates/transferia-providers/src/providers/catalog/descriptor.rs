use serde_json::Value as JsonValue;

use crate::extension::EndpointRole;

#[derive(Clone, Copy)]
pub struct InstallationContract {
    pub output_fields: &'static [&'static str],

    pub required_output_fields: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub(super) struct ProviderRoleDescriptor {
    pub installation: Option<InstallationContract>,

    pub networked: bool,
}

pub(super) struct ProviderDescriptor {
    pub key: &'static str,
    pub title: &'static str,
    pub source: Option<ProviderRoleDescriptor>,
    pub sink: Option<ProviderRoleDescriptor>,
}

const LOGBROKER_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const KAFKA_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["brokers", "security"],
        required_output_fields: &["brokers", "security"],
    }),
    networked: true,
});

const POSTGRES_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const CLICKHOUSE_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["hosts", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["hosts", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const YTSAURUS_ROLE: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const NETWORKED_PLAIN: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: None,
    networked: true,
});
const LOCAL_PLAIN: Option<ProviderRoleDescriptor> = Some(ProviderRoleDescriptor {
    installation: None,
    networked: false,
});

pub(super) static PROVIDERS: &[ProviderDescriptor] = &[
    ProviderDescriptor {
        key: "logbroker",
        title: "Logbroker",
        source: LOGBROKER_ROLE,
        sink: LOGBROKER_ROLE,
    },
    ProviderDescriptor {
        key: "kafka",
        title: "Kafka",
        source: KAFKA_ROLE,
        sink: KAFKA_ROLE,
    },
    ProviderDescriptor {
        key: "postgres",
        title: "PostgreSQL",
        source: POSTGRES_ROLE,
        sink: POSTGRES_ROLE,
    },
    ProviderDescriptor {
        key: "clickhouse",
        title: "ClickHouse",
        source: CLICKHOUSE_ROLE,
        sink: CLICKHOUSE_ROLE,
    },
    ProviderDescriptor {
        key: "s3",
        title: "S3",
        source: NETWORKED_PLAIN,
        sink: NETWORKED_PLAIN,
    },
    ProviderDescriptor {
        key: "iceberg",
        title: "Apache Iceberg",
        source: NETWORKED_PLAIN,
        sink: NETWORKED_PLAIN,
    },
    ProviderDescriptor {
        key: "ytsaurus",
        title: "YTsaurus",
        source: YTSAURUS_ROLE,
        sink: YTSAURUS_ROLE,
    },
    ProviderDescriptor {
        key: "data_generator",
        title: "Data generator (for benchmarks)",
        source: LOCAL_PLAIN,
        sink: None,
    },
    ProviderDescriptor {
        key: "discard",
        title: "Discard (for benchmarks)",
        source: None,
        sink: LOCAL_PLAIN,
    },
];

pub(super) fn provider_descriptor(key: &str) -> Option<&'static ProviderDescriptor> {
    PROVIDERS.iter().find(|provider| provider.key == key)
}

fn provider_role(provider: &str, role: EndpointRole) -> Option<ProviderRoleDescriptor> {
    let descriptor = provider_descriptor(provider)?;
    match role {
        EndpointRole::Source => descriptor.source,
        EndpointRole::Sink => descriptor.sink,
    }
}

pub fn installation_contract(provider: &str, role: EndpointRole) -> Option<InstallationContract> {
    provider_role(provider, role)?.installation
}

pub fn provider_roles() -> impl Iterator<Item = (&'static str, EndpointRole)> {
    PROVIDERS.iter().flat_map(|provider| {
        [
            provider
                .source
                .map(|_| (provider.key, EndpointRole::Source)),
            provider.sink.map(|_| (provider.key, EndpointRole::Sink)),
        ]
        .into_iter()
        .flatten()
    })
}

#[cfg(test)]
pub fn networked_provider_roles() -> impl Iterator<Item = (&'static str, EndpointRole)> {
    PROVIDERS.iter().flat_map(|provider| {
        [
            provider
                .source
                .filter(|role| role.networked)
                .map(|_| (provider.key, EndpointRole::Source)),
            provider
                .sink
                .filter(|role| role.networked)
                .map(|_| (provider.key, EndpointRole::Sink)),
        ]
        .into_iter()
        .flatten()
    })
}

pub fn provider_contracts() -> JsonValue {
    let role = |descriptor: Option<ProviderRoleDescriptor>| {
        descriptor.map(|descriptor| {
            descriptor.installation.map_or_else(
                || {
                    serde_json::json!({
                        "installation": null,
                        "networked": descriptor.networked,
                    })
                },
                |contract| {
                    serde_json::json!({
                        "installation": {
                            "output_fields": contract.output_fields,
                            "required_output_fields": contract.required_output_fields,
                        },
                        "networked": descriptor.networked,
                    })
                },
            )
        })
    };
    JsonValue::Array(
        PROVIDERS
            .iter()
            .map(|provider| {
                serde_json::json!({
                    "key": provider.key,
                    "title": provider.title,
                    "source": role(provider.source),
                    "sink": role(provider.sink),
                })
            })
            .collect(),
    )
}

pub fn provider_supports_role(provider: &str, role: EndpointRole) -> bool {
    provider_role(provider, role).is_some()
}
