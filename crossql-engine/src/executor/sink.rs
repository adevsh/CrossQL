use chrono::{DateTime, Utc};
use polars::lazy::prelude::LazyFrame;
use polars::prelude::{ParquetCompression, ParquetWriter};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCompression {
    Snappy,
    Zstd,
    Gzip,
    Lz4,
}

impl SinkCompression {
    fn to_parquet(self) -> ParquetCompression {
        match self {
            SinkCompression::Snappy => ParquetCompression::Snappy,
            SinkCompression::Zstd => ParquetCompression::Zstd(None),
            SinkCompression::Gzip => ParquetCompression::Gzip(None),
            SinkCompression::Lz4 => ParquetCompression::Lz4Raw,
        }
    }
}

pub fn output_path(
    output_root: &Path,
    pipeline_id: &str,
    version: u32,
    job_id: &str,
    timestamp: DateTime<Utc>,
) -> PathBuf {
    let name = format!(
        "v{}_{}_{}.parquet",
        version,
        job_id,
        timestamp.format("%Y%m%dT%H%M%SZ")
    );
    output_root.join(pipeline_id).join(name)
}

pub fn sink_parquet(
    lf: LazyFrame,
    output_root: &Path,
    pipeline_id: &str,
    version: u32,
    job_id: &str,
    timestamp: DateTime<Utc>,
    compression: SinkCompression,
) -> Result<PathBuf, String> {
    let path = output_path(output_root, pipeline_id, version, job_id, timestamp);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed creating output directory: {}", e))?;
    }

    let mut df = lf
        .collect()
        .map_err(|e| format!("Failed to collect lazy frame: {}", e))?;
    let file = File::create(&path).map_err(|e| format!("Failed to create parquet file: {}", e))?;

    ParquetWriter::new(file)
        .with_compression(compression.to_parquet())
        .finish(&mut df)
        .map_err(|e| format!("Failed to write parquet: {}", e))?;

    Ok(path)
}

pub fn sink_parquet_stream(
    lf: LazyFrame,
    output_root: &Path,
    pipeline_id: &str,
    version: u32,
    job_id: &str,
    timestamp: DateTime<Utc>,
    compression: SinkCompression,
) -> Result<PathBuf, String> {
    sink_parquet(
        lf,
        output_root,
        pipeline_id,
        version,
        job_id,
        timestamp,
        compression,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::lazy::prelude::IntoLazy;
    use polars::prelude::{Column as PolarsColumn, DataFrame, NamedFrom, ScanArgsParquet, Series};
    use uuid::Uuid;

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("crossql-engine-test-{}", Uuid::new_v4()))
    }

    #[test]
    fn test_output_path_format() {
        let path = output_path(
            Path::new("/tmp/out"),
            "orders_join",
            3,
            "job-abc",
            DateTime::parse_from_rfc3339("2026-03-16T04:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        assert_eq!(
            path.to_string_lossy(),
            "/tmp/out/orders_join/v3_job-abc_20260316T040000Z.parquet"
        );
    }

    #[test]
    fn test_sink_creates_output_directory() {
        let root = temp_root();
        let columns: Vec<PolarsColumn> = vec![Series::new("id".into(), &[1_i64, 2, 3]).into()];
        let df = DataFrame::new(3, columns).unwrap();
        let path = sink_parquet(
            df.lazy(),
            &root,
            "p1",
            1,
            "job1",
            Utc::now(),
            SinkCompression::Snappy,
        )
        .unwrap();
        assert!(path.exists());
        assert!(path.parent().unwrap().exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_sink_produces_valid_parquet() {
        let root = temp_root();
        let df = DataFrame::new(3, vec![
            Series::new("id".into(), &[1_i64, 2, 3]).into(),
            Series::new("value".into(), &["a", "b", "c"]).into(),
        ])
        .unwrap();
        let path = sink_parquet(
            df.lazy(),
            &root,
            "p2",
            1,
            "job2",
            Utc::now(),
            SinkCompression::Snappy,
        )
        .unwrap();

        let path_str = path.to_string_lossy().to_string();
        let back = LazyFrame::scan_parquet(path_str.as_str().into(), ScanArgsParquet::default())
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(back.height(), 3);
        let _ = std::fs::remove_dir_all(root);
    }
}
