use polars::prelude::{DataType, TimeUnit};

/// Parse a canonical dtype string into a Polars DataType.
/// Shared by schema mapping and transform operations.
pub fn parse_dtype(dtype: &str) -> Result<DataType, String> {
    match dtype {
        "Int64" => Ok(DataType::Int64),
        "Float64" => Ok(DataType::Float64),
        "Boolean" => Ok(DataType::Boolean),
        "Utf8" => Ok(DataType::String),
        "Datetime" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        _ => Err(format!("Unsupported dtype: {}", dtype)),
    }
}
