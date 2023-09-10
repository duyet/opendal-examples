use std::fs::File;
use std::io::Write;

use anyhow::Result;
use opendal::layers::LoggingLayer;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create example file
    let mut file = File::create("/tmp/hello.txt")?;
    file.write_all(b"Hello")?;

    // Create fs backend builder.
    let mut builder = Fs::default();
    // Set the root for fs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/tmp");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    // Read the file
    let content = op.read("/hello.txt").await?;
    assert_eq!(&content, b"Hello");

    // Write the file
    op.write("/hello.txt", "World").await?;

    // Fetch metadata
    let meta = op.stat("hello.txt").await?;
    let mode = meta.mode();
    let length = meta.content_length();
    println!("mode: {}, length: {}", mode, length);

    // Delete
    op.delete("hello.txt").await?;

    Ok(())
}
