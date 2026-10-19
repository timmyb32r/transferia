//! Order-independent checks against source SQL aggregates. This is a validation
//! pass, outside timed throughput rankings. It verifies row counts, exact
//! signed WatchID sum, raw Title/URL byte totals and UTC EventTime bounds.
use anyhow::{Context,Result};
use arrow::{array::{Array,BinaryArray,LargeBinaryArray,StringArray,LargeStringArray,Int64Array,TimestampSecondArray,TimestampMillisecondArray,TimestampMicrosecondArray},datatypes::DataType,record_batch::RecordBatch};
#[derive(Default,serde::Serialize)]
pub struct Totals {pub rows:u64,pub watchid_sum:i64,pub title_bytes:u64,pub url_bytes:u64,pub min_epoch:Option<i64>,pub max_epoch:Option<i64>}
fn bytes(array:&dyn Array)->Result<u64>{
 anyhow::ensure!(array.null_count()==0,"unexpected NULL in canonical binary column");
 macro_rules! count {($kind:ty)=>{if let Some(a)=array.as_any().downcast_ref::<$kind>(){return Ok(a.iter().flatten().map(|v|v.len()as u64).sum())}}}
 count!(BinaryArray);count!(LargeBinaryArray);count!(StringArray);count!(LargeStringArray);anyhow::bail!("unexpected byte column type {:?}",array.data_type())
}
impl Totals {
 pub fn add(&mut self,batch:&RecordBatch)->Result<()>{
  self.rows+=batch.num_rows()as u64;
  let ids=batch.column_by_name("WatchID").context("missing WatchID")?;
  let ids=ids.as_any().downcast_ref::<Int64Array>().context("WatchID type changed")?;
  anyhow::ensure!(ids.null_count()==0,"unexpected NULL WatchID");for v in ids.values(){self.watchid_sum=self.watchid_sum.wrapping_add(*v);}
  self.title_bytes+=bytes(batch.column_by_name("Title").context("missing Title")?.as_ref())?;
  self.url_bytes+=bytes(batch.column_by_name("URL").context("missing URL")?.as_ref())?;
  let times=batch.column_by_name("EventTime").context("missing EventTime")?;
  anyhow::ensure!(times.null_count()==0,"unexpected NULL EventTime");
  anyhow::ensure!(matches!(times.data_type(),DataType::Timestamp(_,Some(tz)) if tz.as_ref()=="UTC"),"EventTime must retain explicit UTC timezone: {:?}",times.data_type());
  let(mut lo,mut hi)=(i64::MAX,i64::MIN);
  macro_rules! range {($kind:ty,$scale:expr)=>{if let Some(a)=times.as_any().downcast_ref::<$kind>(){for v in a.values(){anyhow::ensure!(*v%$scale==0,"unexpected subsecond timestamp");let v=*v/$scale;lo=lo.min(v);hi=hi.max(v);}}};}
  range!(TimestampSecondArray,1);range!(TimestampMillisecondArray,1000);range!(TimestampMicrosecondArray,1000000);
  anyhow::ensure!(batch.num_rows()==0||lo!=i64::MAX,"unsupported EventTime physical type");
  if batch.num_rows()>0{self.min_epoch=Some(self.min_epoch.map_or(lo,|v|v.min(lo)));self.max_epoch=Some(self.max_epoch.map_or(hi,|v|v.max(hi)));}Ok(())
 }
 pub fn validate(&self)->Result<()>{anyhow::ensure!(self.rows==10000000&&self.watchid_sum== -8759720797844555527&&self.title_bytes==1100028298&&self.url_bytes==862704003&&self.min_epoch==Some(1372708800)&&self.max_epoch==Some(1375300799),"canonical fixture aggregates changed");Ok(())}
}
