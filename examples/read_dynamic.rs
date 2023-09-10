use anyhow::{Context, Result};
use opendal::{Operator, Scheme};
use url::Url;

async fn read_file(path: &str) -> Result<()> {
    // Extract the file name from the path
    let file_name = path.split('/').last().unwrap_or_default();

    // Local file, start with '/'
    let (scheme, args) = if path.starts_with('/') {
        let root = path.trim_end_matches(file_name).to_string();

        (Scheme::Fs, {
            let mut args = std::collections::HashMap::new();
            args.insert("root".to_string(), root);
            args
        })
    } else {
        // Parse the URL to determine the scheme
        let url = Url::parse(path)?;

        // Determine the scheme and create OperatorArgs accordingly
        match url.scheme() {
            "file" => (Scheme::Fs, {
                let mut args = std::collections::HashMap::new();
                args.insert("root".to_string(), url.path().to_string());
                args
            }),
            "http" | "https" => (Scheme::Http, {
                let mut args = std::collections::HashMap::new();
                args.insert(
                    "endpoint".to_string(),
                    url.host_str().unwrap_or_default().to_string(),
                );
                args
            }),
            "s3" => (Scheme::S3, {
                let mut args = std::collections::HashMap::new();
                args.insert(
                    "bucket".to_string(),
                    url.host_str().unwrap_or_default().to_string(),
                );
                if !url.path().is_empty() {
                    args.insert("key".to_string(), url.path()[1..].to_string());
                }
                args
            }),
            _ => {
                return Err(anyhow::anyhow!("Unsupported scheme"));
            }
        }
    };

    // Create an Operator instance and read the CSV file
    let op: Operator = Operator::via_map(scheme, args)?;

    // Read the CSV file
    let content = op
        .read(file_name)
        .await
        .with_context(|| format!("Failed to read from: {}", path))?;

    // For now, just print the data
    println!("{:?}", content);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    read_file("/tmp/aaa").await?;
    read_file("file:///tmp/aaa").await?;
    read_file("https://example.com/path/to/http/file2.csv").await?;
    read_file("s3://bucketname/path/to/s3/file3.csv").await?;

    Ok(())
}
