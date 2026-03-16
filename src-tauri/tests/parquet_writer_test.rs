use crossql_lib::writer::parquet::CrossQLParquetWriter;
use polars::prelude::*;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_parquet_path(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("crossql_{}_{}.parquet", name, stamp))
}

#[doc = "Expected: writer persists a parquet file with supported compression option.\nBehavior: write_dataframe succeeds and the written parquet can be read with the same row count."]
#[test]
fn write_dataframe_writes_readable_parquet_file() {
    let df = df!("id" => &[1_i64, 2, 3], "city" => &["Solo", "Tokyo", "Seoul"]).unwrap();
    let path = unique_parquet_path("writer_ok");

    CrossQLParquetWriter::write_dataframe(
        df.clone(),
        path.to_string_lossy().as_ref(),
        "snappy",
        Some(1024),
    )
    .unwrap();

    let parquet_path = path.to_string_lossy().to_string();
    let read_df = LazyFrame::scan_parquet(parquet_path.as_str().into(), Default::default())
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(read_df.height(), df.height());
    assert!(read_df.column("id").is_ok());
    assert!(read_df.column("city").is_ok());

    let _ = fs::remove_file(path);
}

#[doc = "Expected: writer returns an error when target path cannot be created.\nBehavior: write_dataframe fails with a file creation error for an invalid output path."]
#[test]
fn write_dataframe_returns_error_for_invalid_path() {
    let df = df!("id" => &[1_i64]).unwrap();
    let invalid_path = "/this/path/should/not/exist/output.parquet";

    let result = CrossQLParquetWriter::write_dataframe(df, invalid_path, "snappy", None);
    match result {
        Err(e) => assert!(e.starts_with("Failed to create file:")),
        Ok(_) => panic!("expected write_dataframe to fail for invalid path"),
    }
}
