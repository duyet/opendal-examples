use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let mut map = HashMap::new();
    map.insert("endpoint".to_string(), "archive.ics.uci.edu".to_string());

    let op: Operator = Operator::via_map(Scheme::Http, map)?;

    // Stat
    let _ = op.stat("/static/public/109/wine.zip").await?;

    Ok(())
}
