use polars::prelude::*;
use std::fs::File;
use std::path::Path;

pub struct CrossQLParquetWriter;

impl CrossQLParquetWriter {
    pub fn write_dataframe(
        mut df: DataFrame,
        path: &str,
        compression: &str,
        row_group_size: Option<usize>,
    ) -> Result<(), String> {
        let file = File::create(Path::new(path))
            .map_err(|e| format!("Failed to create file: {}", e))?;

        let mut writer = ParquetWriter::new(file);

        // Map compression string to Polars ParquetCompression
        let compression_algorithm = match compression.to_lowercase().as_str() {
            "snappy" => ParquetCompression::Snappy,
            "gzip" => ParquetCompression::Gzip(None),
            "brotli" => ParquetCompression::Brotli(None),
            "lz4" => ParquetCompression::Lz4Raw,
            "zstd" => ParquetCompression::Zstd(None),
            _ => ParquetCompression::Uncompressed,
        };

        writer = writer.with_compression(compression_algorithm);

        if let Some(size) = row_group_size {
             writer = writer.with_row_group_size(Some(size));
        }

        writer
            .finish(&mut df)
            .map_err(|e| format!("Failed to write Parquet file: {}", e))?;

        Ok(())
    }
}
