//! Bounded parallel IPC decoding for this benchmark's dictionary-free schema.
//! Every record batch is validated by Arrow's ordinary checked reader. This
//! probe only counts verified rows; it does not promise ordered delivery and
//! must not be substituted for a production source. Dictionary messages and
//! schema changes fail explicitly rather than guessing dictionary state.
use std::{collections::HashMap,io::Read,sync::Arc};
use anyhow::{Context,Result};
use arrow::ipc::{root_as_message,MessageHeader};
use arrow::buffer::Buffer;
type Frame=(Vec<u8>,Buffer);
fn frame(reader:&mut impl Read)->Result<Option<Frame>>{
 let mut prefix=[0u8;4];reader.read_exact(&mut prefix).context("truncated IPC stream (missing EOS)")?;
 if prefix==[255;4]{reader.read_exact(&mut prefix)?;}
 let length=u32::from_le_bytes(prefix)as usize;if length==0{return Ok(None)}
 let mut metadata=vec![0;length];reader.read_exact(&mut metadata)?;
 let message=root_as_message(&metadata).map_err(|e|anyhow::anyhow!("invalid IPC metadata: {e:?}"))?;let body_len=usize::try_from(message.bodyLength()).context("negative IPC body size")?;
 let mut body=vec![0;body_len];reader.read_exact(&mut body)?;Ok(Some((metadata,Buffer::from_vec(body))))
}
pub fn count(mut reader:impl Read,workers:usize,validation:Option<Arc<std::sync::Mutex<super::verify::Totals>>>,batch_count:Arc<std::sync::atomic::AtomicU64>,restore_temporal_types:bool)->Result<u64>{
 anyhow::ensure!(workers>0,"positive IPC worker count required");
 let(metadata,_)=frame(&mut reader)?.context("missing IPC schema")?;let message=root_as_message(&metadata).map_err(|e|anyhow::anyhow!("invalid IPC metadata: {e:?}"))?;
 let schema=Arc::new(arrow::ipc::convert::fb_to_schema(message.header_as_schema().context("first IPC message is not schema")?));
 anyhow::ensure!(!schema.fields().iter().any(|f|matches!(f.data_type(),arrow::datatypes::DataType::Dictionary(..))),"dictionary schema unsupported by independent-batch probe");
 std::thread::scope(|scope|->Result<u64>{
  let mut senders=Vec::new();let mut handles=Vec::new();
  for _ in 0..workers {let(tx,rx)=std::sync::mpsc::sync_channel::<Frame>(1);senders.push(tx);let schema=Arc::clone(&schema);let validation=validation.clone();let batch_count=batch_count.clone();
   handles.push(scope.spawn(move||->Result<u64>{let mut rows=0;for(metadata,body)in rx{let message=root_as_message(&metadata).map_err(|e|anyhow::anyhow!("invalid IPC metadata: {e:?}"))?;anyhow::ensure!(message.header_type()==MessageHeader::RecordBatch,"non-record IPC message unsupported");let batch=arrow::ipc::reader::read_record_batch(&body,message.header_as_record_batch().context("invalid record batch header")?,Arc::clone(&schema),&HashMap::new(),None,&message.version())?;let batch=super::canonical_schema::restore(batch,restore_temporal_types)?;batch_count.fetch_add(1,std::sync::atomic::Ordering::Relaxed);if let Some(v)=&validation{v.lock().map_err(|_|anyhow::anyhow!("validation lock poisoned"))?.add(&batch)?;}rows+=batch.num_rows()as u64;}Ok(rows)}));
  }
  let mut sequence=0;let mut producer_error=None;
  loop{match frame(&mut reader){Ok(Some(work))=>{if senders[sequence%workers].send(work).is_err(){producer_error=Some(anyhow::anyhow!("IPC decoder stopped"));break}sequence+=1},Ok(None)=>break,Err(e)=>{producer_error=Some(e);break}}}
  drop(senders);let mut rows=0;for h in handles{rows+=h.join().map_err(|_|anyhow::anyhow!("IPC decoder panicked"))??;}if let Some(e)=producer_error{return Err(e)}Ok(rows)
 })
}
