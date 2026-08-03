//! PQv1 smoke test
use prost::Message;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Endpoint;
use tonic::Request;
use ydb_ch_replicator::config::yaml::{build_credentials_with_token, Config};
use ydb_ch_replicator::pipeline::source::Source;
use ydb_ch_replicator::source::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};
use ydb_ch_replicator::Ydb::Discovery::V1::discovery_service_client::DiscoveryServiceClient;
use ydb_ch_replicator::Ydb::Discovery::V1::{ListEndpointsRequest, ListEndpointsResult};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_file("config_bench.yaml")?;
    let (_creds, token) = build_credentials_with_token(&config.source.auth)?;
    let token = token.unwrap();
    let (_, host, database) = parse_endpoint(&config.source.connection_string)?;

    let ep = Endpoint::from_shared(format!("http://{}", host))?
        .connect_timeout(std::time::Duration::from_secs(10))
        .connect().await?;

    // Discovery uses /Root, not the topic database
    let disc_db = "/Root";
    let t = token.clone();
    let ddb = disc_db.to_string();
    let mut disc = DiscoveryServiceClient::with_interceptor(ep, move |mut req: Request<()>| {
        let _ = req.metadata_mut().insert("x-ydb-auth-ticket", AsciiMetadataValue::try_from(t.as_str()).unwrap());
        let _ = req.metadata_mut().insert("x-ydb-database", AsciiMetadataValue::try_from(ddb.as_str()).unwrap());
        Ok(req)
    });

    println!("--- ListEndpoints(database={disc_db}) ---");
    let resp = match disc.list_endpoints(ListEndpointsRequest { database: disc_db.into() }).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            println!("ListEndpoints failed: {e}");
            return Ok(());
        }
    };
    let op = resp.operation.unwrap();
    let any = op.result.unwrap();
    let result = ListEndpointsResult::decode(any.value.as_slice())?;

    println!("endpoints(f1): {}", result.endpoints.len());
    println!("f2: {}b hex={:02x?} str={:?}",
        result.f2.len(), &result.f2[..], std::str::from_utf8(&result.f2));
    println!("f3: {}b", result.f3.len());
    println!("f4: {}b", result.f4.len());
    println!("f5: {}b first64: {:02x?}", result.f5.len(), &result.f5[..result.f5.len().min(64)]);
    println!("f6: {}b", result.f6.len());
    println!("f7: {}b", result.f7.len());
    println!("f8: {}b", result.f8.len());
    println!("f9: {}b", result.f9.len());
    println!("f10: {}b", result.f10.len());

    // Group by service type
    use std::collections::HashMap;
    let mut svc_count: HashMap<String, usize> = HashMap::new();
    for ep in &result.endpoints {
        for svc in &ep.service {
            *svc_count.entry(svc.clone()).or_default() += 1;
        }
    }
    println!("\nService distribution from field 5:");
    for (svc, count) in &svc_count {
        println!("  {svc}: {count}");
    }

    for (i, ep) in result.endpoints.iter().enumerate().take(5) {
        let addr = format!("{}:{}", ep.address, ep.port);
        let scheme = if ep.ssl { "grpcs" } else { "grpc" };
        let uri = format!("{}://{}", scheme, addr);
        println!("\n--- Try {}: {} (ssl={}, services={:?}) ---", i, uri, ep.ssl, ep.service);

        match PqV1Client::connect(&uri, &database, &config.source.topic_path,
            &config.source.consumer_name, &token, &[0]).await {
            Ok((client, queues)) => {
                println!("✅ HANDSHAKE OK!");
                let mut src = PqV1Source::new(client, queues.into_values().next().unwrap(), 0);
                println!("Waiting for DataBatch...");
                match src.read_batch().await {
                    Ok(b) => {
                        println!("=== {} MESSAGES! ===", b.messages.len());
                        for (i, m) in b.messages.iter().enumerate().take(3) {
                            println!("  [{}] {}...", i,
                                String::from_utf8_lossy(&m.value[..m.value.len().min(120)]));
                        }
                    }
                    Err(e) => println!("Read error: {e}"),
                }
                break;
            }
            Err(e) => {
                let es = e.to_string();
                println!("❌ Connect failed: {:.150}", es);
                // Try with /Root database
                if es.contains("not implemented") {
                    println!("  Trying with /Root database...");
                    match PqV1Client::connect(&uri, "/Root", &config.source.topic_path,
                        &config.source.consumer_name, &token, &[0]).await {
                        Ok((client, queues)) => {
                            println!("✅ HANDSHAKE OK (with /Root DB)!");
                            let mut src = PqV1Source::new(client, queues.into_values().next().unwrap(), 0);
                            match src.read_batch().await {
                                Ok(b) => { println!("=== {} MESSAGES! ===", b.messages.len()); }
                                Err(e2) => println!("Read error: {e2}"),
                            }
                            return Ok(());
                        }
                        Err(e2) => println!("  /Root also failed: {:.100}", e2.to_string()),
                    }
                }
            }
        }
    }

    Ok(())
}
