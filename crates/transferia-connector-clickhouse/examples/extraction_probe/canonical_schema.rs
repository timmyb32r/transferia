//! Explicit schema restoration for this immutable fixture only.
//!
//! ClickHouse exports DateTime as Arrow UInt32 (epoch seconds), omitting its
//! timezone. The fixture's DESCRIBE contract declares all three columns below
//! as non-null DateTime('UTC'). Widening UInt32 to Int64 is exact; attaching the
//! discovered UTC timezone restores that source contract, without shifting time.
//! This is not a general timezone inference or a production Arrow source.
use std::sync::Arc;
use anyhow::{Context,Result};
use arrow::{array::{Array,UInt32Array,TimestampSecondArray},datatypes::{DataType,Schema,TimeUnit},record_batch::RecordBatch};
pub fn restore(batch:RecordBatch,enabled:bool)->Result<RecordBatch>{
 if !enabled{return Ok(batch)}
 let schema=batch.schema();let mut fields=Vec::with_capacity(schema.fields().len());let mut columns=batch.columns().to_vec();let mut changed=false;
 for(index,field)in schema.fields().iter().enumerate(){
  if !matches!(field.name().as_str(),"EventTime"|"ClientEventTime"|"LocalEventTime"){fields.push(field.clone());continue}
  let target=DataType::Timestamp(TimeUnit::Second,Some("UTC".into()));
  match field.data_type(){
   DataType::UInt32=>{
    let values=columns[index].as_any().downcast_ref::<UInt32Array>().context("unexpected temporal array implementation")?;
    anyhow::ensure!(values.null_count()==0,"canonical DateTime is non-null");
    columns[index]=Arc::new(TimestampSecondArray::from_iter_values(values.values().iter().map(|v|i64::from(*v))).with_timezone("UTC"));
    fields.push(Arc::new(field.as_ref().clone().with_data_type(target)));changed=true;
   },
   DataType::Timestamp(_,Some(zone)) if zone.as_ref()=="UTC"=>fields.push(field.clone()),
   other=>anyhow::bail!("unexpected canonical temporal wire type: {other:?}"),
  }
 }
 if !changed{return Ok(batch)}
 Ok(RecordBatch::try_new(Arc::new(Schema::new_with_metadata(fields,schema.metadata().clone())),columns)?)
}
