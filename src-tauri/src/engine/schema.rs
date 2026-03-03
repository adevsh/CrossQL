use polars::lazy::prelude::*;
use polars::prelude::*;

#[derive(serde::Deserialize, Clone)]
pub struct SchemaMapConfig {
    pub columns: Vec<SchemaMapColumnConfig>,
}

#[derive(serde::Deserialize, Clone)]
pub struct SchemaMapColumnConfig {
    pub source: String,
    pub rename: Option<String>,
    pub cast: Option<String>,
    pub null_mode: Option<String>,
    pub fill_value: Option<String>,
}

fn parse_dtype(dtype: &str) -> Result<DataType, String> {
    match dtype {
        "Int64" => Ok(DataType::Int64),
        "Float64" => Ok(DataType::Float64),
        "Boolean" => Ok(DataType::Boolean),
        "Utf8" => Ok(DataType::String),
        "Datetime" => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
        _ => Err(format!("Unsupported dtype: {}", dtype)),
    }
}

fn lit_for_dtype(dtype: &DataType, raw: &str) -> Result<Expr, String> {
    match dtype {
        DataType::Int64 => raw
            .parse::<i64>()
            .map(lit)
            .map_err(|e| format!("Invalid Int64 fill value '{}': {}", raw, e)),
        DataType::Float64 => raw
            .parse::<f64>()
            .map(lit)
            .map_err(|e| format!("Invalid Float64 fill value '{}': {}", raw, e)),
        DataType::Boolean => raw
            .parse::<bool>()
            .map(lit)
            .map_err(|e| format!("Invalid Boolean fill value '{}': {}", raw, e)),
        DataType::String => Ok(lit(raw.to_string())),
        DataType::Datetime(_, _) => {
            let dt = raw
                .parse::<chrono::DateTime<chrono::Utc>>()
                .map_err(|e| format!("Invalid Datetime fill value '{}': {}", raw, e))?;
            Ok(lit(dt.timestamp_millis()).cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
        }
        _ => Err(format!("Unsupported fill dtype: {}", dtype)),
    }
}

pub fn apply_schema_maps(
    mut lf: LazyFrame,
    schema_maps: &[SchemaMapConfig],
) -> Result<(LazyFrame, Vec<String>), String> {
    let mut error_on_null_cols: Vec<String> = Vec::new();

    for sm in schema_maps {
        for c in &sm.columns {
            let source = c.source.trim();
            if source.is_empty() {
                continue;
            }

            let cast_dtype = match c.cast.as_deref().map(str::trim) {
                Some("") | None => None,
                Some(x) => Some(parse_dtype(x)?),
            };

            let mut expr = col(source);
            if let Some(dtype) = &cast_dtype {
                expr = expr.cast(dtype.clone());
            }

            let null_mode = c.null_mode.as_deref().unwrap_or("keep");
            if null_mode == "fill_default" {
                let raw = c.fill_value.as_deref().unwrap_or("").trim();
                if raw.is_empty() {
                    return Err(format!("Fill default requires fill_value for column '{}'", source));
                }
                let fill_expr = if let Some(dtype) = &cast_dtype {
                    lit_for_dtype(dtype, raw)?
                } else {
                    lit(raw.to_string())
                };
                expr = expr.fill_null(fill_expr);
            }

            lf = lf.with_column(expr.alias(source));

            if null_mode == "error" {
                let target_name = c
                    .rename
                    .as_deref()
                    .unwrap_or(source)
                    .trim()
                    .to_string();
                error_on_null_cols.push(target_name);
            }

            if null_mode == "drop_row" {
                lf = lf.filter(col(source).is_not_null());
            }

            if let Some(rename) = c.rename.as_deref().map(str::trim) {
                if !rename.is_empty() && rename != source {
                    lf = lf.rename([source], [rename], false);
                }
            }
        }
    }

    Ok((lf, error_on_null_cols))
}

pub fn validate_no_nulls(df: &DataFrame, cols: &[String]) -> Result<(), String> {
    for c in cols {
        let s = df
            .column(c)
            .map_err(|e| format!("Failed to access column '{}': {}", c, e))?;
        let nulls = s.null_count();
        if nulls > 0 {
            return Err(format!("Column '{}' contains {} null values", c, nulls));
        }
    }
    Ok(())
}
