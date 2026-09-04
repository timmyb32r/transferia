#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array as _, StringArray};
use arrow::datatypes::DataType;
use serde_json::json;
use serde_json::value::RawValue;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::metrics::SourceCounters;
use transferia_core::data::message::SourceBatch;
use transferia_core::data::schema::ARROW_JSON_EXTENSION_NAME;
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;

use super::super::{OpenSearchAuth, OpenSearchClient, OpenSearchConnectionConfig};
use super::config::OpenSearchSourceConfig;
use super::connector::{schema_for_tests, source_enabled_for_tests};
use super::source::{
    build_search_body, close_pits, decode_close_pits, open_index_pit, pit_creation_query,
    search_page, validate_complete_response, validate_hits, HitFields, OpenSearchSource,
    RetryPolicy, SearchHit, SearchHits, SearchResponse, SearchTotal, Shards,
};

fn source_config(indices: &str, page_rows: usize, concurrency: usize, keep_alive: u64) -> String {
    format!(
        "hosts: [localhost]\nport: 9200\ntrusted_plaintext: true\nauth: {{ type: anonymous }}\nindices: {indices}\npage_rows: {page_rows}\nread_concurrency: {concurrency}\npit_keep_alive_ms: {keep_alive}\n"
    )
}

#[test]
fn source_config_requires_exact_distinct_indices_and_positive_limits() {
    let valid: OpenSearchSourceConfig =
        serde_yaml::from_str(&source_config("[{name: logs}]", 100, 2, 10_000)).unwrap();
    valid.validate().unwrap();

    for raw in [
        source_config("[]", 100, 2, 10_000),
        source_config("[{name: 'logs*'}]", 100, 2, 10_000),
        source_config("[{name: logs}, {name: logs}]", 100, 2, 10_000),
        source_config("[{name: logs}]", 0, 2, 10_000),
        source_config("[{name: logs}]", 100, 0, 10_000),
        source_config("[{name: logs}]", 100, 2, 0),
    ] {
        let config: OpenSearchSourceConfig = serde_yaml::from_str(&raw).unwrap();
        assert!(config.validate().is_err(), "{raw}");
    }

    for retry in [
        "retry_initial_ms: 0\n",
        "retry_initial_ms: 10\nretry_max_ms: 9\n",
        "retry_max_attempts: 0\n",
    ] {
        let raw = format!("{}{retry}", source_config("[{name: logs}]", 100, 2, 10_000));
        let config: OpenSearchSourceConfig = serde_yaml::from_str(&raw).unwrap();
        assert!(config.validate().is_err(), "{raw}");
    }
}

#[test]
fn initial_source_configuration_uses_measured_bounded_concurrency() {
    let mut initial = super::initial_config();
    initial["hosts"] = json!(["example.test"]);
    initial["auth"] = json!({ "type": "anonymous" });
    let config: OpenSearchSourceConfig =
        serde_json::from_value(initial).expect("valid initial config");
    assert_eq!(config.read_concurrency, 2);
    config
        .validate()
        .expect("complete source config must validate");
}

#[test]
fn source_schema_preserves_document_identity_routing_and_exact_json() {
    let schema = schema_for_tests();
    assert_eq!(schema.columns.len(), 4);
    assert_eq!(schema.columns[0].name, "_id");
    assert_eq!(schema.columns[0].data_type, DataType::Utf8);
    assert!(!schema.columns[0].nullable);
    assert!(schema.columns[0].primary_key);
    assert_eq!(schema.columns[0].max_length, Some(512));
    assert_eq!(schema.columns[1].name, "_routing");
    assert!(schema.columns[1].nullable);
    assert_eq!(
        schema.columns[2].arrow_extension_name,
        Some(ARROW_JSON_EXTENSION_NAME)
    );
    assert_eq!(schema.columns[3].name, "_routing_key");
    assert_eq!(schema.columns[3].data_type, DataType::Utf8);
    assert!(!schema.columns[3].nullable);
    assert!(schema.columns[3].primary_key);
    assert_eq!(schema.columns[3].max_length, None);
}

#[test]
fn source_mapping_defaults_to_enabled_and_preserves_explicit_disable() {
    assert!(source_enabled_for_tests(json!({"mappings": {}})).unwrap());
    assert!(source_enabled_for_tests(json!({"mappings": {"_source": {}}})).unwrap());
    assert!(!source_enabled_for_tests(json!({
        "mappings": {"_source": {"enabled": false}}
    }))
    .unwrap());
}

fn response(hits: Vec<SearchHit>) -> SearchResponse {
    SearchResponse {
        timed_out: false,
        shards: Shards {
            total: 1,
            successful: 1,
            skipped: 0,
            failed: 0,
            failures: Vec::new(),
        },
        hits: SearchHits {
            total: SearchTotal {
                value: hits.len() as u64,
                relation: "eq".to_owned(),
            },
            hits,
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "test fixtures pass owned JSON values that are consumed into raw source text"
)]
fn hit(id: &str, index: &str, sort: u64, source: serde_json::Value) -> SearchHit {
    SearchHit {
        index: index.to_owned(),
        id: id.to_owned(),
        routing: None,
        fields: HitFields::default(),
        source: RawValue::from_string(source.to_string()).unwrap(),
        sort: vec![json!(sort)],
    }
}

#[test]
fn partial_timed_out_failed_and_inexact_pages_are_rejected() {
    assert!(validate_complete_response(&response(Vec::new())).is_ok());

    let mut value = response(Vec::new());
    value.timed_out = true;
    assert!(validate_complete_response(&value).is_err());

    let mut value = response(Vec::new());
    value.shards.failed = 1;
    assert!(validate_complete_response(&value).is_err());

    let mut value = response(Vec::new());
    value.shards.successful = 0;
    assert!(validate_complete_response(&value).is_err());

    let mut value = response(Vec::new());
    value.shards.failures.push(json!({"reason": "boom"}));
    assert!(validate_complete_response(&value).is_err());

    let mut value = response(Vec::new());
    value.hits.total.relation = "gte".to_owned();
    assert!(validate_complete_response(&value).is_err());
}

#[test]
fn hit_validation_rejects_index_source_routing_and_cursor_drift() {
    validate_hits(
        "logs",
        0,
        None,
        &[hit("a", "logs", 1, json!({"payload": 1}))],
    )
    .unwrap();
    assert!(validate_hits(
        "logs",
        0,
        None,
        &[hit("a", "other", 1, json!({"payload": 1}))]
    )
    .is_err());
    assert!(validate_hits("logs", 0, None, &[hit("a", "logs", 1, json!(null))]).is_err());
    assert!(validate_hits(
        "logs",
        0,
        Some(1),
        &[hit("a", "logs", 1, json!({"payload": 1}))]
    )
    .is_err());

    let mut conflicting = hit("doc", "logs", 1, json!({"value": 1}));
    conflicting.routing = Some("metadata".to_owned());
    conflicting.fields.routing.push("stored".to_owned());
    assert!(validate_hits("logs", 0, None, &[conflicting]).is_err());

    let mut multiple = hit("doc", "logs", 1, json!({"value": 1}));
    multiple.fields.routing = vec!["one".to_owned(), "two".to_owned()];
    assert!(validate_hits("logs", 0, None, &[multiple]).is_err());
}

#[test]
fn hits_become_lossless_document_rows_with_routing_columns() {
    let mut first = hit("doc-1", "logs", 1, json!({"nested": {"n": 1}}));
    first.fields.routing.push("tenant-a".to_owned());
    let second = hit("doc-2", "logs", 2, json!({"text": "hello"}));
    let batch = super::source::hits_to_batch("logs", 7, 11, vec![first, second]).unwrap();
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.value(0), "doc-1");
    assert_eq!(ids.value(1), "doc-2");
    let routing = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(routing.value(0), "tenant-a");
    assert!(routing.is_null(1));
    let source = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(source.value(0)).unwrap(),
        json!({"nested": {"n": 1}})
    );
    let routing_key = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(routing_key.value(0), "tenant-a");
    assert_eq!(routing_key.value(1), "doc-2");
}

#[test]
fn raw_source_preserves_numeric_lexemes_beyond_json_number_precision() {
    let source = r#"{"integer":18446744073709551616000,"decimal":0.123456789012345678901}"#;
    let hit = SearchHit {
        index: "logs".to_owned(),
        id: "doc".to_owned(),
        routing: None,
        fields: HitFields::default(),
        source: RawValue::from_string(source.to_owned()).unwrap(),
        sort: vec![json!(1)],
    };
    let batch = super::source::hits_to_batch("logs", 0, 0, vec![hit]).unwrap();
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), source);
}

#[test]
fn pit_creation_explicitly_forbids_partial_snapshots() {
    assert_eq!(
        pit_creation_query("60000ms"),
        vec![
            ("keep_alive", "60000ms".to_owned()),
            ("allow_partial_pit_creation", "false".to_owned())
        ]
    );
}

#[test]
fn close_response_must_acknowledge_the_exact_pit() {
    let pits = vec![Arc::from("pit-1"), Arc::from("pit-2")];
    let closed = decode_close_pits(
        br#"{"pits":[{"successful":true,"pit_id":"pit-1"},{"successful":false,"pit_id":"pit-2"}]}"#,
        &pits,
    )
    .unwrap();
    assert_eq!(closed, std::iter::once("pit-1".to_owned()).collect());
    assert!(decode_close_pits(
        br#"{"pits":[{"successful":true,"pit_id":"pit-1"},{"successful":true,"pit_id":"other"}]}"#,
        &pits,
    )
    .is_err());
    assert!(
        decode_close_pits(br#"{"pits":[{"successful":true,"pit_id":"pit-1"}]}"#, &pits,).is_err()
    );
}

#[test]
fn search_body_preserves_the_complete_slice_cursor() {
    let first = build_search_body("pit", "60000ms", 100, 2, 4, Some(42)).unwrap();
    let retry = build_search_body("pit", "60000ms", 100, 2, 4, Some(42)).unwrap();
    assert_eq!(first, retry);
    let body: serde_json::Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(body["pit"]["id"], "pit");
    assert_eq!(body["slice"], json!({"id": 2, "max": 4}));
    assert_eq!(body["search_after"], json!([42]));
    assert_eq!(body["stored_fields"], json!(["_routing"]));
    assert_eq!(body["sort"], json!([{"_doc": "asc"}]));
}

#[test]
fn search_body_omits_the_single_slice_clause() {
    let body = build_search_body("pit", "60000ms", 100, 0, 1, None).unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("slice").is_none());
    assert!(body.get("search_after").is_none());
}

#[tokio::test]
async fn transient_page_retry_reuses_the_identical_request_body_and_cursor() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for response in [
            "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_owned(),
            format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                VALID_EMPTY_PAGE.len(),
                VALID_EMPTY_PAGE
            ),
        ] {
            let (mut stream, _) = server.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            bodies.push(request_body(&request).to_vec());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        bodies
    });
    let client = OpenSearchClient::new(&OpenSearchConnectionConfig {
        hosts: vec!["127.0.0.1".to_owned()],
        port: address.port(),
        trusted_plaintext: true,
        tls_ca_file: None,
        auth: OpenSearchAuth::Anonymous,
        request_timeout_ms: 1_000,
        max_response_bytes: 1_048_576,
    })
    .unwrap();
    let page = search_page(
        client,
        Arc::from("60000ms"),
        Arc::from("pit-1"),
        100,
        2,
        4,
        Some(42),
        RetryPolicy::new(1, 1, 2),
        Arc::new(SourceCounters::new()),
    )
    .await;
    assert!(page.result.is_ok());
    let bodies = task.await.unwrap();
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0], bodies[1]);
    let body: serde_json::Value = serde_json::from_slice(&bodies[0]).unwrap();
    assert_eq!(body["pit"]["id"], "pit-1");
    assert_eq!(body["slice"], json!({"id": 2, "max": 4}));
    assert_eq!(body["search_after"], json!([42]));
}

#[tokio::test]
async fn repeated_shutdown_retries_only_contexts_not_yet_confirmed_closed() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for body in [
            r#"{"pits":[{"successful":true,"pit_id":"pit-a"},{"successful":false,"pit_id":"pit-b"}]}"#,
            r#"{"pits":[{"successful":true,"pit_id":"pit-b"}]}"#,
        ] {
            let (mut stream, _) = server.accept().await.unwrap();
            let request = read_http_request(&mut stream).await;
            bodies.push(request_body(&request).to_vec());
            stream
                .write_all(http_response(body).as_bytes())
                .await
                .unwrap();
        }
        bodies
    });
    let client = test_client(address.port());
    let mut pits = vec![Arc::from("pit-a"), Arc::from("pit-b")];
    assert!(close_pits(
        &client,
        &mut pits,
        RetryPolicy::new(1, 1, 1),
        &SourceCounters::new(),
    )
    .await
    .is_err());
    assert_eq!(pits, [Arc::from("pit-b")]);
    close_pits(
        &client,
        &mut pits,
        RetryPolicy::new(1, 1, 1),
        &SourceCounters::new(),
    )
    .await
    .unwrap();
    assert!(pits.is_empty());
    let bodies = task.await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bodies[0]).unwrap(),
        json!({"pit_id": ["pit-a", "pit-b"]})
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bodies[1]).unwrap(),
        json!({"pit_id": ["pit-b"]})
    );
}

#[tokio::test]
async fn cancellation_drains_started_pit_creation_and_closes_the_returned_context() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let (request_seen, request_received) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = server.accept().await.unwrap();
        let create = read_http_request(&mut stream).await;
        request_seen.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let opened = r#"{"pit_id":"pit-cancelled","_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}"#;
        stream
            .write_all(http_response(opened).as_bytes())
            .await
            .unwrap();

        let (mut stream, _) = server.accept().await.unwrap();
        let close = read_http_request(&mut stream).await;
        let closed = r#"{"pits":[{"successful":true,"pit_id":"pit-cancelled"}]}"#;
        stream
            .write_all(http_response(closed).as_bytes())
            .await
            .unwrap();
        (create, close)
    });
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let client = test_client(address.port());
    let open_task = tokio::spawn(async move {
        open_index_pit(
            &client,
            "logs",
            1,
            "60000ms",
            RetryPolicy::new(1, 1, 1),
            &task_cancellation,
            &Arc::new(SourceCounters::new()),
        )
        .await
    });
    request_received.await.unwrap();
    cancellation.cancel();
    assert!(open_task.await.unwrap().is_err());
    let (create, close) = server_task.await.unwrap();
    let create = String::from_utf8(create).unwrap();
    assert!(create.starts_with("POST /logs/_search/point_in_time?"));
    assert!(create.contains("allow_partial_pit_creation=false"));
    assert!(!create.contains("preference="));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(request_body(&close)).unwrap(),
        json!({"pit_id": ["pit-cancelled"]})
    );
}

#[tokio::test]
async fn incomplete_index_pit_creation_closes_the_returned_context() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (mut stream, _) = server.accept().await.unwrap();
        read_http_request(&mut stream).await;
        let opened = r#"{"pit_id":"pit-incomplete","_shards":{"total":2,"successful":1,"skipped":0,"failed":1}}"#;
        stream
            .write_all(http_response(opened).as_bytes())
            .await
            .unwrap();
        let (mut stream, _) = server.accept().await.unwrap();
        let close = read_http_request(&mut stream).await;
        let closed = r#"{"pits":[{"successful":true,"pit_id":"pit-incomplete"}]}"#;
        stream
            .write_all(http_response(closed).as_bytes())
            .await
            .unwrap();
        close
    });

    let result = open_index_pit(
        &test_client(address.port()),
        "logs",
        3,
        "60000ms",
        RetryPolicy::new(1, 1, 1),
        &CancellationToken::new(),
        &Arc::new(SourceCounters::new()),
    )
    .await;
    assert!(result.is_err());
    let close = server_task.await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(request_body(&close)).unwrap(),
        json!({"pit_id": ["pit-incomplete"]})
    );
}

#[tokio::test]
async fn concurrency_one_uses_one_pit_and_reads_every_slice_before_continuing() {
    let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let mut requests = Vec::new();
        let (mut stream, _) = server.accept().await.unwrap();
        requests.push(read_http_request(&mut stream).await);
        let opened = r#"{"pit_id":"one-index-pit","_shards":{"total":3,"successful":3,"skipped":0,"failed":0}}"#;
        stream
            .write_all(http_response(opened).as_bytes())
            .await
            .unwrap();
        for slice in 0..3 {
            let (mut stream, _) = server.accept().await.unwrap();
            requests.push(read_http_request(&mut stream).await);
            let page = format!(
                "{{\"timed_out\":false,\"_shards\":{{\"total\":3,\"successful\":3,\"skipped\":0,\"failed\":0}},\"hits\":{{\"total\":{{\"value\":2,\"relation\":\"eq\"}},\"hits\":[{{\"_index\":\"logs\",\"_id\":\"slice-{slice}\",\"_source\":{{\"slice\":{slice}}},\"sort\":[0]}}]}}}}"
            );
            stream
                .write_all(http_response(&page).as_bytes())
                .await
                .unwrap();
        }
        let (mut stream, _) = server.accept().await.unwrap();
        requests.push(read_http_request(&mut stream).await);
        let closed = r#"{"pits":[{"successful":true,"pit_id":"one-index-pit"}]}"#;
        stream
            .write_all(http_response(closed).as_bytes())
            .await
            .unwrap();
        requests
    });

    let mut source = OpenSearchSource::open(
        test_client(address.port()),
        Arc::from("logs"),
        0,
        1,
        1,
        3,
        "60000ms".to_owned(),
        1,
        1,
        1,
        CancellationToken::new(),
        PipelineMemory::new(1_048_576),
        Arc::new(SourceCounters::new()),
    )
    .await
    .unwrap();
    for _ in 0..3 {
        assert!(matches!(
            source.read_batch().await.unwrap(),
            SourceBatch::Typed { source_rows: 1, .. }
        ));
    }
    source.shutdown().await.unwrap();

    let requests = server_task.await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| String::from_utf8_lossy(request)
                .starts_with("POST /logs/_search/point_in_time?"))
            .count(),
        1
    );
    let slice_order = requests[1..4]
        .iter()
        .map(|request| {
            let body = serde_json::from_slice::<serde_json::Value>(request_body(request)).unwrap();
            assert_eq!(body["pit"]["id"], "one-index-pit");
            assert_eq!(body["slice"]["max"], 3);
            body["slice"]["id"].as_u64().unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(slice_order, [0, 1, 2]);
}

const VALID_EMPTY_PAGE: &str = r#"{"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"#;

fn test_client(port: u16) -> OpenSearchClient {
    OpenSearchClient::new(&OpenSearchConnectionConfig {
        hosts: vec!["127.0.0.1".to_owned()],
        port,
        trusted_plaintext: true,
        tls_ca_file: None,
        auth: OpenSearchAuth::Anonymous,
        request_timeout_ms: 1_000,
        max_response_bytes: 1_048_576,
    })
    .unwrap()
}

fn http_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "request closed before its body completed");
        request.extend_from_slice(&chunk[..read]);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .map_or(0, |value| value.parse::<usize>().unwrap());
        if request.len() >= header_end + 4 + content_length {
            return request;
        }
    }
}

fn request_body(request: &[u8]) -> &[u8] {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap();
    &request[header_end + 4..]
}
