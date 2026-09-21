//! Benchmark-only bounded local-preview adapter. No connector logic is replaced.
//! Copies exactly the immutable fixture's known row count, adds explicit Flow
//! fixture commits every 1000 documents, then terminates the capture process
//! group. This is a local preview measurement, not the managed Flow data plane.
use std::{env,fs::File,io::{BufRead,BufReader,BufWriter,Write},process::{Command,Stdio}};
fn main() -> Result<(),Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len()!=5 { return Err("expected rows output spec task".into()); }
    let rows:usize=args[1].parse()?;
    let mut child=Command::new("setsid").args(["/work/flowctl","preview","--source",&args[3],"--name",&args[4],"-o","json"]).stdout(Stdio::piped()).spawn()?;
    let pid=child.id();
    let outcome=(|| -> Result<(),Box<dyn std::error::Error>> {
        let mut input=BufReader::with_capacity(1<<20,child.stdout.take().ok_or("missing stdout")?);
        let mut output=BufWriter::with_capacity(1<<20,File::create(&args[2])?);
        let mut line=Vec::new();
        for index in 0..rows {
            line.clear();
            if input.read_until(b'\n',&mut line)?==0 { return Err(format!("capture EOF after {index}/{rows} rows").into()); }
            if line.first()!=Some(&b'[') { return Err("unexpected non-document preview output".into()); }
            output.write_all(&line)?;
            if (index+1)%1000==0 { output.write_all(b"{\"commit\":true}\n")?; }
        }
        if rows%1000!=0 { output.write_all(b"{\"commit\":true}\n")?; }
        output.flush()?;
        Ok(())
    })();
    let _=Command::new("kill").args(["-TERM","--",&format!("-{pid}")]).status();
    let _=child.wait();
    outcome
}
