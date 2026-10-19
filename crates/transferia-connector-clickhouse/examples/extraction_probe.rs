//! Finite-query extraction probe. Raw and decoded results are different metrics.
//! Credentials are read from a private JSON file and never emitted. Every query
//! completes normally; duration is a minimum, not a mid-file cancellation limit.
use std::{io::{Read,Cursor},time::{Duration,Instant}};
use anyhow::{Context,Result};
#[path="extraction_probe/parallel_arrow.rs"]
mod parallel_arrow;
#[path="extraction_probe/verify.rs"]
mod verify;
#[path="extraction_probe/canonical_schema.rs"]
mod canonical_schema;
use bytes::Bytes;
use futures_util::StreamExt;
use serde::Deserialize;
use transferia_connector_support::outbound_http::{OutboundHttpClient,NetworkPolicy};
#[derive(Deserialize)]
struct Config { host:String, password:String, #[serde(default="user")] user:String, #[serde(default="database")] database:String, ca:String, query:String, format:String, codec:String, http_codec:String, mode:String, seconds:u64, expected_rows:u64, threads:usize, decode_threads:usize, #[serde(default)] parallel_arrow:bool, #[serde(default)] verify_values:bool, #[serde(default)] binary_strings:bool, #[serde(default)] http_level:Option<i64>, #[serde(default)] format_level:Option<i64>, #[serde(default="row_groups")] row_group_rows:usize, #[serde(default)] block_rows:Option<usize>, #[serde(default)] preferred_block_bytes:Option<u64>, #[serde(default)] server_wait:bool, #[serde(default)] restore_temporal_types:bool }
fn row_groups()->usize{250000}
fn user()->String{"user1".into()} fn database()->String{"db1".into()}
struct ChannelReader { rx:tokio::sync::mpsc::Receiver<Bytes>, current:Cursor<Bytes> }
impl Read for ChannelReader { fn read(&mut self,b:&mut[u8])->std::io::Result<usize>{loop{let n=self.current.read(b)?;if n!=0{return Ok(n)}match self.rx.blocking_recv(){Some(x)=>self.current=Cursor::new(x),None=>return Ok(0)}}} }
#[tokio::main]
async fn main()->Result<()> {
 let path=std::env::args().nth(1).context("private config path required")?;
 let c:Config=serde_json::from_slice(&std::fs::read(path)?)?;
 anyhow::ensure!(c.threads>0 && c.decode_threads>0 && c.seconds>0 && c.row_group_rows>0,"positive execution parameters required");
 anyhow::ensure!(c.block_rows.is_none_or(|v|v>0),"positive block row count required");
 anyhow::ensure!(matches!(c.mode.as_str(),"raw"|"decode"),"invalid mode");
 anyhow::ensure!(!c.parallel_arrow || (c.format=="ArrowStream" && c.mode=="decode"),"parallel IPC requires decoded ArrowStream");
 anyhow::ensure!(c.mode!="decode" || c.http_codec=="none","HTTP-compressed decoding is not implemented in this probe");
 anyhow::ensure!(!c.verify_values || (c.mode=="decode" && c.expected_rows==10000000),"value verification requires the complete canonical fixture");
 anyhow::ensure!(c.format!="NativeTCP" || c.mode=="decode","NativeTCP supports decoded mode only");
 anyhow::ensure!(!c.restore_temporal_types || (c.mode=="decode" && c.expected_rows==10000000 && c.query.contains("FROM db1.cb_w105_10m")),"schema restoration is restricted to the canonical fixture");
 if c.format=="NativeTCP" {
  use clickhouse_arrow::{ClientBuilder,CompressionMethod};
  let codec=match c.codec.as_str(){"none"=>CompressionMethod::None,"lz4"=>CompressionMethod::LZ4,"zstd"=>CompressionMethod::ZSTD,_=>anyhow::bail!("invalid native codec")};
  let client=ClientBuilder::new().with_destination(format!("{}:9440",c.host)).with_database(&c.database).with_username(&c.user).with_password(&c.password).with_tls(true).with_cafile(&c.ca).with_compression(codec).with_setting("network_compression_method",if c.codec=="zstd"{"zstd"}else{"lz4"}).with_setting("max_threads",c.threads as i64).with_setting("max_block_size",65409i64).with_setting("preferred_block_size_bytes",0i64).build_arrow().await?;
  let start=Instant::now();let mut rows=0u64;let mut cycles=0u64;let mut observations=Vec::new();
  loop {let t=Instant::now();let mut response=client.query(c.query.clone(),None).await?;let mut n=0u64;let mut memory_bytes=0u64;let mut batch_count=0u64;let mut totals=verify::Totals::default();while let Some(b)=response.next().await{let b=canonical_schema::restore(b?,c.restore_temporal_types)?;batch_count+=1;if c.verify_values{totals.add(&b)?;}n+=b.num_rows()as u64;memory_bytes+=b.get_array_memory_size()as u64;}anyhow::ensure!(n==c.expected_rows,"native row mismatch");if c.verify_values{totals.validate()?;}rows+=n;cycles+=1;observations.push(serde_json::json!({"cycle":cycles,"wall_s":t.elapsed().as_secs_f64(),"decoded_rows":n,"decoded_batches":batch_count,"arrow_memory_bytes":memory_bytes,"validation":if c.verify_values{Some(totals)}else{None}}));eprintln!("completed native cycle {cycles}: {:.2}s",t.elapsed().as_secs_f64());if start.elapsed()>=Duration::from_secs(c.seconds){break}}
  let elapsed=start.elapsed().as_secs_f64();println!("{}",serde_json::json!({"format":c.format,"codec":c.codec,"mode":"decode","threads":c.threads,"rows":rows,"cycles":cycles,"wall_s":elapsed,"rows_per_s":rows as f64/elapsed,"observations":observations}));return Ok(())
 }
 let roots=reqwest::Certificate::from_pem_bundle(&std::fs::read(&c.ca)?)?;
 let client=OutboundHttpClient::new(Duration::from_secs(1800),roots,NetworkPolicy::AllowPrivateNetworks)?;
 let mut cycles=0u64;let mut rows=0u64;let mut bytes=0u64;let start=Instant::now();let mut observations=Vec::new();
 loop {
  let restore_temporal_types=c.restore_temporal_types;
  let mut url=reqwest::Url::parse(&format!("https://{}:8443/",c.host))?;
  let fc=if c.codec=="lz4"&&matches!(c.format.as_str(),"ArrowStream"|"Arrow"){"lz4_frame"}else{&c.codec};
  url.query_pairs_mut().append_pair("wait_end_of_query",if c.server_wait{"1"}else{"0"}).append_pair("max_threads",&c.threads.to_string()).append_pair("output_format_arrow_compression_method",if matches!(c.format.as_str(),"ArrowStream"|"Arrow"){fc}else{"none"}).append_pair("output_format_parquet_compression_method",if c.format=="Parquet"{fc}else{"none"}).append_pair("output_format_parquet_row_group_size",&c.row_group_rows.to_string()).append_pair("enable_http_compression",if c.http_codec=="none"{"0"}else{"1"});
  url.query_pairs_mut().append_pair("output_format_orc_compression_method",if c.format=="ORC"{fc}else{"none"});
  url.query_pairs_mut().append_pair("output_format_arrow_string_as_string",if c.binary_strings{"0"}else{"1"}).append_pair("output_format_parquet_string_as_string",if c.binary_strings{"0"}else{"1"});
  if let Some(rows)=c.block_rows{url.query_pairs_mut().append_pair("max_block_size",&rows.to_string());}
  if let Some(bytes)=c.preferred_block_bytes{url.query_pairs_mut().append_pair("preferred_block_size_bytes",&bytes.to_string());}
  if let Some(level)=c.http_level{url.query_pairs_mut().append_pair("http_zlib_compression_level",&level.to_string());}
  if let Some(level)=c.format_level{url.query_pairs_mut().append_pair("output_format_compression_level",&level.to_string());}
  let sql=format!("{} FORMAT {}",c.query,c.format);let t=Instant::now();
  let response=client.request(reqwest::Method::POST,url).configure(|b|b.header("X-ClickHouse-User",&c.user).header("X-ClickHouse-Key",&c.password).header("X-ClickHouse-Database",&c.database).header("Accept-Encoding",if c.http_codec=="none"{"identity"}else{&c.http_codec}).body(sql)).send().await?;
  anyhow::ensure!(response.status().is_success(),"server returned status {}",response.status());
  let query_id=response.headers().get("x-clickhouse-query-id").and_then(|v|v.to_str().ok()).map(str::to_owned);
  let server_summary=response.headers().get("x-clickhouse-summary").and_then(|v|v.to_str().ok()).map(str::to_owned);
  let encoding=response.headers().get("content-encoding").and_then(|v|v.to_str().ok()).unwrap_or("identity").to_string();
  let headers_s=t.elapsed().as_secs_f64();let body_finished_s;
  let mut stream=response.bytes_stream();let mut received=0u64;
  let mut decoded=0u64;let batch_count=std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));let validation=c.verify_values.then(||std::sync::Arc::new(std::sync::Mutex::new(verify::Totals::default())));
  if c.mode=="decode"&&c.format=="ArrowStream"&&c.http_codec=="none" {
   let(tx,rx)=tokio::sync::mpsc::channel(8);
   let parallel=c.parallel_arrow;let workers=c.decode_threads;let validation=validation.clone();let batch_count=batch_count.clone();
   let task=tokio::task::spawn_blocking(move||->Result<u64>{let reader=ChannelReader{rx,current:Cursor::new(Bytes::new())};if parallel{return parallel_arrow::count(reader,workers,validation,batch_count,restore_temporal_types)}let mut n=0;for b in arrow::ipc::reader::StreamReader::try_new(reader,None)?{let b=canonical_schema::restore(b?,restore_temporal_types)?;batch_count.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.add(&b)?;}n+=b.num_rows()as u64;}Ok(n)});
   while let Some(chunk)=stream.next().await{let chunk=chunk?;received+=chunk.len()as u64;tx.send(chunk).await.map_err(|_|anyhow::anyhow!("decoder stopped"))?;}
   drop(tx);body_finished_s=t.elapsed().as_secs_f64();decoded=task.await??;
  }else{
   let mut payload=Vec::new();while let Some(chunk)=stream.next().await{let chunk=chunk?;received+=chunk.len()as u64;if c.mode=="decode"{payload.extend_from_slice(&chunk)}}
   body_finished_s=t.elapsed().as_secs_f64();
   if c.mode=="decode"{
    anyhow::ensure!(encoding=="identity","HTTP-compressed decoding not implemented for this cell");
    let format=c.format.clone();let concurrency=c.decode_threads;let validation=validation.clone();let batch_count=batch_count.clone();
    decoded=tokio::task::spawn_blocking(move||->Result<u64>{let data=Bytes::from(payload);if format=="Parquet"{
     use parquet::arrow::arrow_reader::{ArrowReaderMetadata,ArrowReaderOptions,ParquetRecordBatchReaderBuilder};
     let metadata=ArrowReaderMetadata::load(&data,ArrowReaderOptions::default())?;let groups=metadata.metadata().num_row_groups();let next=std::sync::atomic::AtomicUsize::new(0);
     std::thread::scope(|scope|->Result<u64>{let mut tasks=Vec::new();for _ in 0..concurrency.min(groups.max(1)){let data=data.clone();let metadata=metadata.clone();let next=&next;let validation=validation.clone();let batch_count=batch_count.clone();tasks.push(scope.spawn(move||->Result<u64>{let mut n=0;loop{let g=next.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if g>=groups{break}for b in ParquetRecordBatchReaderBuilder::new_with_metadata(data.clone(),metadata.clone()).with_row_groups(vec![g]).with_batch_size(65409).build()?{let b=canonical_schema::restore(b?,restore_temporal_types)?;batch_count.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.add(&b)?;}n+=b.num_rows()as u64;}}Ok(n)}));}let mut n=0;for task in tasks{n+=task.join().map_err(|_|anyhow::anyhow!("decoder panicked"))??;}Ok(n)})
    }else if format=="Arrow"{let mut n=0;for b in arrow::ipc::reader::FileReader::try_new(Cursor::new(data),None)?{let b=canonical_schema::restore(b?,restore_temporal_types)?;batch_count.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.add(&b)?;}n+=b.num_rows()as u64;}Ok(n)}else if format=="ArrowStream"{let mut n=0;for b in arrow::ipc::reader::StreamReader::try_new(Cursor::new(data),None)?{let b=canonical_schema::restore(b?,restore_temporal_types)?;batch_count.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.add(&b)?;}n+=b.num_rows()as u64;}Ok(n)}else{anyhow::bail!("unsupported decoded format")}}).await??;
   }
  }
  if c.mode=="decode"{anyhow::ensure!(decoded==c.expected_rows,"decoded row count mismatch: {decoded}")};
  if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.validate()?;}
  let validation_result=match &validation{Some(v)=>Some(serde_json::to_value(&*v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?)?),None=>None};
  cycles+=1;rows+=c.expected_rows;bytes+=received;
  observations.push(serde_json::json!({"cycle":cycles,"wall_s":t.elapsed().as_secs_f64(),"bytes":received,"headers_s":headers_s,"body_finished_s":body_finished_s,"post_transfer_decode_s":t.elapsed().as_secs_f64()-body_finished_s,"decoded_rows":decoded,"decoded_batches":batch_count.load(std::sync::atomic::Ordering::Relaxed),"content_encoding":encoding,"server_summary":server_summary,"query_id":query_id,"validation":validation_result}));
  eprintln!("completed cycle {cycles}: {:.2}s, {received} bytes",t.elapsed().as_secs_f64());
  if start.elapsed()>=Duration::from_secs(c.seconds){break}
 }
 let elapsed=start.elapsed().as_secs_f64();println!("{}",serde_json::json!({"format":c.format,"codec":c.codec,"http_codec":c.http_codec,"mode":c.mode,"parallel_arrow":c.parallel_arrow,"binary_strings":c.binary_strings,"verify_values":c.verify_values,"http_level":c.http_level,"format_level":c.format_level,"row_group_rows":c.row_group_rows,"block_rows":c.block_rows,"preferred_block_bytes":c.preferred_block_bytes,"server_wait":c.server_wait,"restore_temporal_types":c.restore_temporal_types,"threads":c.threads,"decode_threads":c.decode_threads,"rows":rows,"bytes":bytes,"cycles":cycles,"wall_s":elapsed,"rows_per_s":rows as f64/elapsed,"mib_per_s":bytes as f64/elapsed/1048576.,"observations":observations}));Ok(())
}
