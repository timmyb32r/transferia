use serde_json::Value as JsonValue;

use crate::extension::EndpointRole;

#[derive(Clone, Copy)]
pub struct InstallationContract {
    pub output_fields: &'static [&'static str],

    pub required_output_fields: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub(super) struct ConnectorRoleDescriptor {
    pub installation: Option<InstallationContract>,

    pub networked: bool,
}

pub(super) struct ConnectorDescriptor {
    pub key: &'static str,
    pub title: &'static str,
    pub source: Option<ConnectorRoleDescriptor>,
    pub sink: Option<ConnectorRoleDescriptor>,
}

const LOGBROKER_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const KAFKA_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["brokers", "security"],
        required_output_fields: &["brokers", "security"],
    }),
    networked: true,
});

const POSTGRES_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const MYSQL_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["host", "port", "trusted_plaintext", "tls_ca_file"],
        required_output_fields: &["host", "port", "trusted_plaintext"],
    }),
    networked: true,
});

const CLICKHOUSE_SOURCE_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &[
            "hosts",
            "port",
            "http_port",
            "trusted_plaintext",
            "tls_ca_file",
        ],
        required_output_fields: &["hosts", "port", "http_port", "trusted_plaintext"],
    }),
    networked: true,
});

const CLICKHOUSE_SINK_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &[
            "hosts",
            "port",
            "http_port",
            "trusted_plaintext",
            "tls_ca_file",
            "data_host_count",
        ],
        required_output_fields: &["hosts", "port", "http_port", "trusted_plaintext"],
    }),
    networked: true,
});

const YTSAURUS_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &[
            "host",
            "port",
            "trusted_plaintext",
            "trusted_native_rpc_plaintext",
        ],
        required_output_fields: &[
            "host",
            "port",
            "trusted_plaintext",
            "trusted_native_rpc_plaintext",
        ],
    }),
    networked: true,
});

const S3_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["bucket", "endpoint", "region", "credentials"],
        required_output_fields: &["bucket", "endpoint", "region", "credentials"],
    }),
    networked: true,
});

const ICEBERG_ROLE: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: Some(InstallationContract {
        output_fields: &["storage"],
        required_output_fields: &["storage"],
    }),
    networked: true,
});

const LOCAL_PLAIN: Option<ConnectorRoleDescriptor> = Some(ConnectorRoleDescriptor {
    installation: None,
    networked: false,
});

pub(super) static CONNECTORS: &[ConnectorDescriptor] = &[
    ConnectorDescriptor {
        key: "logbroker",
        title: "Logbroker",
        source: LOGBROKER_ROLE,
        sink: LOGBROKER_ROLE,
    },
    ConnectorDescriptor {
        key: "kafka",
        title: "Kafka",
        source: KAFKA_ROLE,
        sink: KAFKA_ROLE,
    },
    ConnectorDescriptor {
        key: "mysql",
        title: "MySQL",
        source: MYSQL_ROLE,
        sink: None,
    },
    ConnectorDescriptor {
        key: "postgres",
        title: "PostgreSQL",
        source: POSTGRES_ROLE,
        sink: POSTGRES_ROLE,
    },
    ConnectorDescriptor {
        key: "clickhouse",
        title: "ClickHouse",
        source: CLICKHOUSE_SOURCE_ROLE,
        sink: CLICKHOUSE_SINK_ROLE,
    },
    ConnectorDescriptor {
        key: "s3",
        title: "S3",
        source: S3_ROLE,
        sink: S3_ROLE,
    },
    ConnectorDescriptor {
        key: "iceberg",
        title: "Apache Iceberg",
        source: ICEBERG_ROLE,
        sink: ICEBERG_ROLE,
    },
    ConnectorDescriptor {
        key: "ytsaurus",
        title: "YTsaurus",
        source: YTSAURUS_ROLE,
        sink: YTSAURUS_ROLE,
    },
    ConnectorDescriptor {
        key: "data_generator",
        title: "Data generator (for benchmarks)",
        source: LOCAL_PLAIN,
        sink: None,
    },
    ConnectorDescriptor {
        key: "discard",
        title: "Discard (for benchmarks)",
        source: None,
        sink: LOCAL_PLAIN,
    },
];

pub(super) fn connector_descriptor(key: &str) -> Option<&'static ConnectorDescriptor> {
    CONNECTORS.iter().find(|connector| connector.key == key)
}

fn connector_role(connector: &str, role: EndpointRole) -> Option<ConnectorRoleDescriptor> {
    let descriptor = connector_descriptor(connector)?;
    match role {
        EndpointRole::Source => descriptor.source,
        EndpointRole::Sink => descriptor.sink,
    }
}

pub fn installation_contract(connector: &str, role: EndpointRole) -> Option<InstallationContract> {
    connector_role(connector, role)?.installation
}

pub fn connector_roles() -> impl Iterator<Item = (&'static str, EndpointRole)> {
    CONNECTORS.iter().flat_map(|connector| {
        [
            connector
                .source
                .map(|_| (connector.key, EndpointRole::Source)),
            connector.sink.map(|_| (connector.key, EndpointRole::Sink)),
        ]
        .into_iter()
        .flatten()
    })
}

#[cfg(test)]
pub fn networked_connector_roles() -> impl Iterator<Item = (&'static str, EndpointRole)> {
    CONNECTORS.iter().flat_map(|connector| {
        [
            connector
                .source
                .filter(|role| role.networked)
                .map(|_| (connector.key, EndpointRole::Source)),
            connector
                .sink
                .filter(|role| role.networked)
                .map(|_| (connector.key, EndpointRole::Sink)),
        ]
        .into_iter()
        .flatten()
    })
}

pub fn connector_contracts() -> JsonValue {
    let role = |descriptor: Option<ConnectorRoleDescriptor>| {
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
        CONNECTORS
            .iter()
            .map(|connector| {
                serde_json::json!({
                    "key": connector.key,
                    "title": connector.title,
                    "source": role(connector.source),
                    "sink": role(connector.sink),
                })
            })
            .collect(),
    )
}

pub fn connector_supports_role(connector: &str, role: EndpointRole) -> bool {
    connector_role(connector, role).is_some()
}
