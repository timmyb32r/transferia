use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use bytes::BytesMut;
use fallible_iterator::FallibleIterator as _;
use postgres_protocol::authentication::{self, sasl};
use postgres_protocol::message::backend::{ErrorResponseBody, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio_postgres::tls::TlsConnect as _;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;

use crate::connectors::postgres::common::{
    quote_identifier, validate_identifier, PostgresConnectionConfig,
};
use crate::connectors::postgres::src_stream::PostgresSystemIdentity;

trait AsyncIo: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

type BoxedIo = Box<dyn AsyncIo>;

#[derive(Debug)]
pub struct AmbiguousReplicationSlotCreation {
    source: anyhow::Error,
}

impl std::fmt::Display for AmbiguousReplicationSlotCreation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "PostgreSQL replication slot creation result is ambiguous; the configured permanent \
             slot may have been created without a usable exported snapshot and requires a \
             deliberate reset before retrying",
        )
    }
}

impl std::error::Error for AmbiguousReplicationSlotCreation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// The atomic result of creating a logical slot and exporting its snapshot.
///
/// `PostgreSQL` keeps the snapshot valid only while the replication session
/// which exported it remains open. Keeping this value alive therefore owns the
/// server-side snapshot lifetime.
#[must_use = "dropping the bootstrap closes the session that owns the exported snapshot"]
pub struct ReplicationSlotBootstrap {
    _session: BoxedIo,

    pub(crate) slot: String,

    pub(crate) consistent_lsn: u64,

    pub(crate) snapshot: String,

    pub(crate) plugin: String,
}

impl std::fmt::Debug for ReplicationSlotBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicationSlotBootstrap")
            .field("slot", &self.slot)
            .field("consistent_lsn", &self.consistent_lsn)
            .field("snapshot", &self.snapshot)
            .field("plugin", &self.plugin)
            .finish_non_exhaustive()
    }
}

impl ReplicationSlotBootstrap {
    pub(crate) async fn identify_system(
        connection: &PostgresConnectionConfig,
        cancellation: &CancellationToken,
        operation_timeout: Duration,
    ) -> anyhow::Result<PostgresSystemIdentity> {
        validate_connection(connection, operation_timeout)?;
        observe_external_request(
            "postgres",
            "identify_system",
            run_bounded(cancellation, operation_timeout, async {
                let stream = open_replication_stream(connection).await?;
                identify_session(
                    stream,
                    &connection.username,
                    &connection.password,
                    &connection.database,
                )
                .await
            }),
        )
        .await
    }

    pub(crate) async fn create(
        connection: &PostgresConnectionConfig,
        slot: &str,
        plugin: &str,
        expected_system: &PostgresSystemIdentity,
        cancellation: &CancellationToken,
        operation_timeout: Duration,
    ) -> anyhow::Result<Self> {
        validate_connection(connection, operation_timeout)?;
        validate_identifier("replication slot", slot)?;
        validate_identifier("logical decoding plugin", plugin)?;
        anyhow::ensure!(
            expected_system.database == connection.database,
            "PostgreSQL replication bootstrap identity belongs to a different database"
        );

        let request_may_have_been_sent = Arc::new(AtomicBool::new(false));
        let result = observe_external_request(
            "postgres",
            "bootstrap_replication_slot",
            run_bounded(cancellation, operation_timeout, async {
                let stream = open_replication_stream(connection).await?;
                bootstrap_session(
                    stream,
                    &connection.username,
                    &connection.password,
                    &connection.database,
                    slot,
                    plugin,
                    expected_system,
                    Arc::clone(&request_may_have_been_sent),
                )
                .await
            }),
        )
        .await;
        match result {
            Err(error)
                if request_may_have_been_sent.load(Ordering::Acquire)
                    && error
                        .downcast_ref::<AmbiguousReplicationSlotCreation>()
                        .is_none()
                    && error.downcast_ref::<PostgresServerError>().is_none() =>
            {
                Err(AmbiguousReplicationSlotCreation { source: error }.into())
            }
            result => result,
        }
    }
}

fn validate_connection(
    connection: &PostgresConnectionConfig,
    operation_timeout: Duration,
) -> anyhow::Result<()> {
    connection.validate()?;
    anyhow::ensure!(
        !connection.username.contains('\0')
            && !connection.database.contains('\0')
            && !connection.password.contains('\0'),
        "PostgreSQL replication bootstrap credentials and database must not contain NUL"
    );
    anyhow::ensure!(
        !operation_timeout.is_zero(),
        "PostgreSQL replication bootstrap timeout must be positive"
    );
    Ok(())
}

async fn run_bounded<T>(
    cancellation: &CancellationToken,
    operation_timeout: Duration,
    operation: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            anyhow::bail!("PostgreSQL replication bootstrap cancelled")
        }
        result = tokio::time::timeout(operation_timeout, operation) => {
            result.map_err(|_| anyhow::anyhow!(
                "PostgreSQL replication bootstrap timed out after {} ms",
                operation_timeout.as_millis()
            ))?
        }
    }
}

async fn open_replication_stream(connection: &PostgresConnectionConfig) -> anyhow::Result<BoxedIo> {
    let mut socket = TcpStream::connect((connection.host.as_str(), connection.port))
        .await
        .context("failed to connect PostgreSQL replication bootstrap socket")?;
    socket.set_nodelay(true)?;
    if connection.trusted_plaintext {
        return Ok(Box::new(socket));
    }

    let mut request = BytesMut::new();
    frontend::ssl_request(&mut request);
    socket.write_all(&request).await?;
    let mut response = [0_u8; 1];
    socket.read_exact(&mut response).await?;
    anyhow::ensure!(
        response[0] == b'S',
        "PostgreSQL server refused required TLS for replication bootstrap"
    );

    drop(rustls::crypto::aws_lc_rs::default_provider().install_default());
    let mut roots = rustls::RootCertStore::empty();
    let native = tokio::task::spawn_blocking(rustls_native_certs::load_native_certs)
        .await
        .context("failed to load native certificates for PostgreSQL replication TLS")?;
    for certificate in native.certs {
        roots.add(certificate)?;
    }
    if let Some(path) = &connection.tls_ca_file {
        let bytes = tokio::fs::read(path).await?;
        let mut reader = std::io::BufReader::new(bytes.as_slice());
        for certificate in rustls_pemfile::certs(&mut reader) {
            roots.add(certificate?)?;
        }
    }
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
    let connector =
        <tokio_postgres_rustls::MakeRustlsConnect as tokio_postgres::tls::MakeTlsConnect<
            TcpStream,
        >>::make_tls_connect(&mut connector, &connection.host)
        .map_err(|error| anyhow::anyhow!("failed to configure PostgreSQL TLS: {error}"))?;
    let stream = connector
        .connect(socket)
        .await
        .map_err(|error| anyhow::anyhow!("PostgreSQL replication TLS handshake failed: {error}"))?;
    Ok(Box::new(stream))
}

async fn bootstrap_session(
    mut stream: BoxedIo,
    username: &str,
    password: &str,
    database: &str,
    slot: &str,
    plugin: &str,
    expected_system: &PostgresSystemIdentity,
    request_may_have_been_sent: Arc<AtomicBool>,
) -> anyhow::Result<ReplicationSlotBootstrap> {
    start_replication_session(&mut stream, username, password, database).await?;
    let actual_system = identify_system_on_session(&mut stream).await?;
    anyhow::ensure!(
        actual_system == *expected_system,
        "PostgreSQL system or database identity changed before replication slot creation"
    );

    create_slot_on_session(stream, slot, plugin, request_may_have_been_sent).await
}

async fn identify_session(
    mut stream: BoxedIo,
    username: &str,
    password: &str,
    database: &str,
) -> anyhow::Result<PostgresSystemIdentity> {
    start_replication_session(&mut stream, username, password, database).await?;
    let identity = identify_system_on_session(&mut stream).await?;
    anyhow::ensure!(
        identity.database == database,
        "PostgreSQL IDENTIFY_SYSTEM returned a different database"
    );
    Ok(identity)
}

async fn start_replication_session(
    stream: &mut BoxedIo,
    username: &str,
    password: &str,
    database: &str,
) -> anyhow::Result<()> {
    let mut write_buffer = BytesMut::new();
    frontend::startup_message(
        [
            ("client_encoding", "UTF8"),
            ("user", username),
            ("database", database),
            ("replication", "database"),
        ],
        &mut write_buffer,
    )?;
    stream.write_all(&write_buffer).await?;

    let mut read_buffer = BytesMut::new();
    authenticate(stream, &mut read_buffer, username, password).await?;
    read_startup_completion(stream, &mut read_buffer).await?;
    Ok(())
}

async fn identify_system_on_session(
    stream: &mut BoxedIo,
) -> anyhow::Result<PostgresSystemIdentity> {
    let mut write_buffer = BytesMut::new();
    frontend::query("IDENTIFY_SYSTEM", &mut write_buffer)?;
    stream.write_all(&write_buffer).await?;
    let mut read_buffer = BytesMut::new();
    read_identify_system_response(stream, &mut read_buffer).await
}

async fn create_slot_on_session(
    mut stream: BoxedIo,
    slot: &str,
    plugin: &str,
    request_may_have_been_sent: Arc<AtomicBool>,
) -> anyhow::Result<ReplicationSlotBootstrap> {
    let mut write_buffer = BytesMut::new();
    let mut read_buffer = BytesMut::new();

    frontend::query(
        &format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL {} EXPORT_SNAPSHOT",
            quote_identifier(slot),
            quote_identifier(plugin)
        ),
        &mut write_buffer,
    )?;
    request_may_have_been_sent.store(true, Ordering::Release);
    stream
        .write_all(&write_buffer)
        .await
        .map_err(|source| AmbiguousReplicationSlotCreation {
            source: source.into(),
        })?;
    let response = read_slot_response(&mut stream, &mut read_buffer)
        .await
        .map_err(|source| {
            if source.downcast_ref::<PostgresServerError>().is_some() {
                source
            } else {
                AmbiguousReplicationSlotCreation { source }.into()
            }
        })?;
    let validate_response = || -> anyhow::Result<u64> {
        anyhow::ensure!(
            response.slot == slot,
            "PostgreSQL created a logical slot with an unexpected name"
        );
        anyhow::ensure!(
            response.plugin == plugin,
            "PostgreSQL created a logical slot with an unexpected output plugin"
        );
        validate_snapshot_id(&response.snapshot)?;
        parse_consistent_lsn(&response.consistent_point)
    };
    let consistent_lsn = validate_response()
        .map_err(|source| anyhow::Error::from(AmbiguousReplicationSlotCreation { source }))?;

    Ok(ReplicationSlotBootstrap {
        _session: stream,
        slot: response.slot,
        consistent_lsn,
        snapshot: response.snapshot,
        plugin: response.plugin,
    })
}

async fn authenticate(
    stream: &mut BoxedIo,
    read_buffer: &mut BytesMut,
    username: &str,
    password: &str,
) -> anyhow::Result<()> {
    match read_message(stream, read_buffer).await? {
        Message::AuthenticationOk => return Ok(()),
        Message::AuthenticationCleartextPassword => {
            send_password(stream, password.as_bytes()).await?;
        }
        Message::AuthenticationMd5Password(body) => {
            let password =
                authentication::md5_hash(username.as_bytes(), password.as_bytes(), body.salt());
            send_password(stream, password.as_bytes()).await?;
        }
        Message::AuthenticationSasl(body) => {
            authenticate_scram(stream, read_buffer, body, password).await?;
        }
        Message::ErrorResponse(body) => return Err(server_error(&body)?),
        Message::AuthenticationKerberosV5
        | Message::AuthenticationScmCredential
        | Message::AuthenticationGss
        | Message::AuthenticationSspi
        | Message::AuthenticationGssContinue(_) => {
            anyhow::bail!("PostgreSQL requested an unsupported authentication mechanism")
        }
        _ => anyhow::bail!("PostgreSQL sent an unexpected message during authentication"),
    }

    match read_message(stream, read_buffer).await? {
        Message::AuthenticationOk => Ok(()),
        Message::ErrorResponse(body) => Err(server_error(&body)?),
        _ => anyhow::bail!("PostgreSQL did not complete authentication"),
    }
}

async fn send_password(stream: &mut BoxedIo, password: &[u8]) -> anyhow::Result<()> {
    let mut message = BytesMut::new();
    frontend::password_message(password, &mut message)?;
    stream.write_all(&message).await?;
    Ok(())
}

async fn authenticate_scram(
    stream: &mut BoxedIo,
    read_buffer: &mut BytesMut,
    body: postgres_protocol::message::backend::AuthenticationSaslBody,
    password: &str,
) -> anyhow::Result<()> {
    let mut supports_scram = false;
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next()? {
        if mechanism == sasl::SCRAM_SHA_256 {
            supports_scram = true;
        }
    }
    anyhow::ensure!(
        supports_scram,
        "PostgreSQL did not offer the supported SCRAM-SHA-256 mechanism"
    );

    let mut scram =
        sasl::ScramSha256::new(password.as_bytes(), sasl::ChannelBinding::unsupported());
    let mut message = BytesMut::new();
    frontend::sasl_initial_response(sasl::SCRAM_SHA_256, scram.message(), &mut message)?;
    stream.write_all(&message).await?;
    let body = match read_message(stream, read_buffer).await? {
        Message::AuthenticationSaslContinue(body) => body,
        Message::ErrorResponse(body) => return Err(server_error(&body)?),
        _ => anyhow::bail!("PostgreSQL did not continue SCRAM authentication"),
    };
    scram.update(body.data())?;

    message.clear();
    frontend::sasl_response(scram.message(), &mut message)?;
    stream.write_all(&message).await?;
    let body = match read_message(stream, read_buffer).await? {
        Message::AuthenticationSaslFinal(body) => body,
        Message::ErrorResponse(body) => return Err(server_error(&body)?),
        _ => anyhow::bail!("PostgreSQL did not finish SCRAM authentication"),
    };
    scram.finish(body.data())?;
    Ok(())
}

async fn read_startup_completion(
    stream: &mut BoxedIo,
    read_buffer: &mut BytesMut,
) -> anyhow::Result<()> {
    loop {
        match read_message(stream, read_buffer).await? {
            Message::ParameterStatus(_)
            | Message::BackendKeyData(_)
            | Message::NoticeResponse(_) => {}
            Message::ReadyForQuery(body) => {
                anyhow::ensure!(
                    body.status() == b'I',
                    "PostgreSQL replication bootstrap session is not idle after startup"
                );
                return Ok(());
            }
            Message::ErrorResponse(body) => return Err(server_error(&body)?),
            _ => anyhow::bail!("PostgreSQL sent an unexpected startup message"),
        }
    }
}

struct SlotResponse {
    slot: String,
    consistent_point: String,
    snapshot: String,
    plugin: String,
}

async fn read_identify_system_response(
    stream: &mut BoxedIo,
    read_buffer: &mut BytesMut,
) -> anyhow::Result<PostgresSystemIdentity> {
    let mut described = false;
    let mut identity = None;
    let mut completed = false;
    loop {
        match read_message(stream, read_buffer).await? {
            Message::RowDescription(body) => {
                anyhow::ensure!(
                    !described,
                    "PostgreSQL repeated IDENTIFY_SYSTEM response description"
                );
                validate_identify_system_description(&body)?;
                described = true;
            }
            Message::DataRow(body) => {
                anyhow::ensure!(
                    described,
                    "PostgreSQL sent IDENTIFY_SYSTEM data before its description"
                );
                anyhow::ensure!(
                    identity.is_none(),
                    "PostgreSQL returned multiple IDENTIFY_SYSTEM rows"
                );
                identity = Some(parse_identify_system_row(&body)?);
            }
            Message::CommandComplete(body) => {
                anyhow::ensure!(
                    described && identity.is_some() && !completed,
                    "PostgreSQL completed IDENTIFY_SYSTEM before returning exactly one row"
                );
                anyhow::ensure!(
                    body.tag()? == "IDENTIFY_SYSTEM",
                    "PostgreSQL returned an unexpected IDENTIFY_SYSTEM command tag"
                );
                completed = true;
            }
            Message::ReadyForQuery(body) => {
                anyhow::ensure!(
                    completed && body.status() == b'I',
                    "PostgreSQL IDENTIFY_SYSTEM did not complete in idle state"
                );
                return identity
                    .ok_or_else(|| anyhow::anyhow!("PostgreSQL returned no IDENTIFY_SYSTEM row"));
            }
            Message::NoticeResponse(_) | Message::ParameterStatus(_) => {}
            Message::ErrorResponse(body) => return Err(server_error(&body)?),
            _ => anyhow::bail!("PostgreSQL sent an unexpected IDENTIFY_SYSTEM message"),
        }
    }
}

fn validate_identify_system_description(
    body: &postgres_protocol::message::backend::RowDescriptionBody,
) -> anyhow::Result<()> {
    let fields = body.fields().collect::<Vec<_>>()?;
    let expected = [
        ("systemid", 25, -1),
        ("timeline", 20, 8),
        ("xlogpos", 25, -1),
        ("dbname", 25, -1),
    ];
    anyhow::ensure!(
        fields.len() == expected.len(),
        "PostgreSQL IDENTIFY_SYSTEM response has {} columns instead of {}",
        fields.len(),
        expected.len()
    );
    for (field, (expected_name, expected_oid, expected_size)) in fields.iter().zip(expected) {
        anyhow::ensure!(
            field.name() == expected_name
                && field.table_oid() == 0
                && field.column_id() == 0
                && field.type_oid() == expected_oid
                && field.type_size() == expected_size
                && field.type_modifier() == -1
                && field.format() == 0,
            "PostgreSQL IDENTIFY_SYSTEM response column '{expected_name}' does not match the protocol contract"
        );
    }
    Ok(())
}

fn parse_identify_system_row(
    body: &postgres_protocol::message::backend::DataRowBody,
) -> anyhow::Result<PostgresSystemIdentity> {
    let ranges = body.ranges().collect::<Vec<_>>()?;
    anyhow::ensure!(
        ranges.len() == 4,
        "PostgreSQL IDENTIFY_SYSTEM row has {} values instead of 4",
        ranges.len()
    );
    let values = ranges
        .into_iter()
        .map(|range| {
            let range = range
                .ok_or_else(|| anyhow::anyhow!("PostgreSQL IDENTIFY_SYSTEM row contains NULL"))?;
            Ok(std::str::from_utf8(&body.buffer()[range])?.to_owned())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let [system_identifier, timeline, xlogpos, database]: [String; 4] = values
        .try_into()
        .map_err(|_| anyhow::anyhow!("PostgreSQL IDENTIFY_SYSTEM row has an invalid shape"))?;
    let system_identifier = system_identifier.parse::<u64>().map_err(|_| {
        anyhow::anyhow!("PostgreSQL IDENTIFY_SYSTEM returned an invalid system identifier")
    })?;
    anyhow::ensure!(
        system_identifier != 0,
        "PostgreSQL IDENTIFY_SYSTEM returned an invalid system identifier"
    );
    let timeline = timeline
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("PostgreSQL IDENTIFY_SYSTEM returned an invalid timeline"))?;
    anyhow::ensure!(
        timeline != 0,
        "PostgreSQL IDENTIFY_SYSTEM returned an invalid timeline"
    );
    parse_consistent_lsn(&xlogpos)
        .context("PostgreSQL IDENTIFY_SYSTEM returned an invalid WAL position")?;
    anyhow::ensure!(
        !database.is_empty() && !database.contains('\0'),
        "PostgreSQL IDENTIFY_SYSTEM returned an invalid database name"
    );
    Ok(PostgresSystemIdentity {
        system_identifier,
        database,
    })
}

async fn read_slot_response(
    stream: &mut BoxedIo,
    read_buffer: &mut BytesMut,
) -> anyhow::Result<SlotResponse> {
    let mut described = false;
    let mut row = None;
    let mut completed = false;
    loop {
        match read_message(stream, read_buffer).await? {
            Message::RowDescription(body) => {
                anyhow::ensure!(!described, "PostgreSQL repeated slot response description");
                validate_slot_description(&body)?;
                described = true;
            }
            Message::DataRow(body) => {
                anyhow::ensure!(
                    described,
                    "PostgreSQL sent slot data before its description"
                );
                anyhow::ensure!(row.is_none(), "PostgreSQL returned multiple slot rows");
                row = Some(parse_slot_row(&body)?);
            }
            Message::CommandComplete(body) => {
                anyhow::ensure!(
                    described && row.is_some() && !completed,
                    "PostgreSQL completed slot creation before returning exactly one row"
                );
                anyhow::ensure!(
                    body.tag()? == "CREATE_REPLICATION_SLOT",
                    "PostgreSQL returned an unexpected slot command tag"
                );
                completed = true;
            }
            Message::ReadyForQuery(body) => {
                anyhow::ensure!(
                    completed && body.status() == b'I',
                    "PostgreSQL slot creation did not complete in idle state"
                );
                return row.ok_or_else(|| anyhow::anyhow!("PostgreSQL returned no slot row"));
            }
            Message::NoticeResponse(_) | Message::ParameterStatus(_) => {}
            Message::ErrorResponse(body) => return Err(server_error(&body)?),
            _ => anyhow::bail!("PostgreSQL sent an unexpected slot creation message"),
        }
    }
}

fn validate_slot_description(
    body: &postgres_protocol::message::backend::RowDescriptionBody,
) -> anyhow::Result<()> {
    let fields = body.fields().collect::<Vec<_>>()?;
    let expected = [
        "slot_name",
        "consistent_point",
        "snapshot_name",
        "output_plugin",
    ];
    anyhow::ensure!(
        fields.len() == expected.len(),
        "PostgreSQL slot response has {} columns instead of {}",
        fields.len(),
        expected.len()
    );
    for (field, expected_name) in fields.iter().zip(expected) {
        anyhow::ensure!(
            field.name() == expected_name
                && field.table_oid() == 0
                && field.column_id() == 0
                && field.type_oid() == 25
                && field.type_size() == -1
                && field.type_modifier() == -1
                && field.format() == 0,
            "PostgreSQL slot response column '{expected_name}' does not match the protocol contract"
        );
    }
    Ok(())
}

fn parse_slot_row(
    body: &postgres_protocol::message::backend::DataRowBody,
) -> anyhow::Result<SlotResponse> {
    let ranges = body.ranges().collect::<Vec<_>>()?;
    anyhow::ensure!(
        ranges.len() == 4,
        "PostgreSQL slot row has {} values instead of 4",
        ranges.len()
    );
    let values = ranges
        .into_iter()
        .map(|range| {
            let range =
                range.ok_or_else(|| anyhow::anyhow!("PostgreSQL slot row contains NULL"))?;
            Ok(std::str::from_utf8(&body.buffer()[range])?.to_owned())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let [slot, consistent_point, snapshot, plugin]: [String; 4] = values
        .try_into()
        .map_err(|_| anyhow::anyhow!("PostgreSQL slot row has an invalid shape"))?;
    Ok(SlotResponse {
        slot,
        consistent_point,
        snapshot,
        plugin,
    })
}

async fn read_message(stream: &mut BoxedIo, buffer: &mut BytesMut) -> anyhow::Result<Message> {
    loop {
        if let Some(message) = Message::parse(buffer)? {
            return Ok(message);
        }
        anyhow::ensure!(
            stream.read_buf(buffer).await? != 0,
            "PostgreSQL closed the replication bootstrap session unexpectedly"
        );
    }
}

#[derive(Debug)]
struct PostgresServerError {
    code: String,
}

impl std::fmt::Display for PostgresServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "PostgreSQL server rejected replication bootstrap (SQLSTATE {})",
            self.code
        )
    }
}

impl std::error::Error for PostgresServerError {}

fn server_error(body: &ErrorResponseBody) -> anyhow::Result<anyhow::Error> {
    let mut code = None;
    let mut fields = body.fields();
    while let Some(field) = fields.next()? {
        if field.type_() == b'C' {
            let value = std::str::from_utf8(field.value_bytes())?;
            if value.len() == 5
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            {
                code = Some(value.to_owned());
            }
        }
    }
    Ok(PostgresServerError {
        code: code.unwrap_or_else(|| "unknown".to_owned()),
    }
    .into())
}

fn validate_snapshot_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "PostgreSQL returned an invalid exported snapshot identifier"
    );
    Ok(())
}

fn parse_consistent_lsn(value: &str) -> anyhow::Result<u64> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL returned an invalid consistent LSN"))?;
    anyhow::ensure!(
        !high.is_empty()
            && !low.is_empty()
            && high.bytes().all(|byte| byte.is_ascii_hexdigit())
            && low.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "PostgreSQL returned an invalid consistent LSN"
    );
    let high = u32::from_str_radix(high, 16)
        .map_err(|_| anyhow::anyhow!("PostgreSQL returned an invalid consistent LSN"))?;
    let low = u32::from_str_radix(low, 16)
        .map_err(|_| anyhow::anyhow!("PostgreSQL returned an invalid consistent LSN"))?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

#[cfg(test)]
#[path = "tests/bootstrap.rs"]
mod tests;
