use calamine::{open_workbook_auto, DataType as XlsDataType, Reader};
use polars::prelude::{Column as PolarsColumn, *};
use std::path::Path;

pub struct FileConnector;

fn validate_headers(df: &DataFrame) -> Result<(), String> {
    for (i, name) in df.get_column_names().iter().enumerate() {
        if name.trim().is_empty() {
            return Err(format!(
                "Column {} has an empty header — all columns must have names. \
                 Please add headers to your file before importing.",
                i + 1
            ));
        }
    }
    Ok(())
}

fn dtype_str(dt: &DataType) -> String {
    match dt.to_string().as_str() {
        s if s.starts_with("Int") || s.starts_with("UInt") => "Int64".to_string(),
        s if s.starts_with("Float") => "Float64".to_string(),
        "Boolean" => "Boolean".to_string(),
        "Date" => "Date".to_string(),
        s if s.starts_with("Datetime") => "Datetime".to_string(),
        _ => "Utf8".to_string(),
    }
}

fn read_xlsx(path: &str) -> Result<DataFrame, String> {
    let mut workbook = open_workbook_auto(path)
        .map_err(|e| format!("Failed to open file: {}", e))?;

    let sheet_names = workbook.sheet_names().to_vec();
    let sheet_name = sheet_names
        .first()
        .ok_or_else(|| "File has no sheets".to_string())?
        .clone();

    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|e| format!("Failed to read sheet '{}': {}", sheet_name, e))?;

    let mut rows = range.rows();

    // First row = headers
    let headers: Vec<String> = match rows.next() {
        Some(r) => r
            .iter()
            .map(|c| match c {
                XlsDataType::String(s) => s.trim().to_string(),
                XlsDataType::Float(f) => f.to_string(),
                XlsDataType::Int(i) => i.to_string(),
                _ => String::new(),
            })
            .collect(),
        None => return Err("File is empty — no header row found".to_string()),
    };

    if headers.is_empty() {
        return Err("File has no columns".to_string());
    }

    let mut col_data: Vec<Vec<Option<String>>> = vec![Vec::new(); headers.len()];
    for row in rows {
        for (ci, cell) in row.iter().enumerate() {
            if ci < headers.len() {
                let val = match cell {
                    XlsDataType::Empty => None,
                    XlsDataType::String(s) if s.trim().is_empty() => None,
                    XlsDataType::String(s) => Some(s.clone()),
                    XlsDataType::Float(f) => Some(f.to_string()),
                    XlsDataType::Int(i) => Some(i.to_string()),
                    XlsDataType::Bool(b) => Some(b.to_string()),
                    XlsDataType::DateTime(f) => Some(f.to_string()),
                    _ => None,
                };
                col_data[ci].push(val);
            }
        }
        for ci in row.len()..headers.len() {
            col_data[ci].push(None);
        }
    }

    let series_vec: Vec<PolarsColumn> = headers
        .iter()
        .zip(col_data.iter())
        .map(|(name, vals)| Series::new(name.as_str().into(), vals.clone()).into())
        .collect();

    DataFrame::new(series_vec).map_err(|e| format!("Failed to build DataFrame: {}", e))
}

impl FileConnector {
    /// Read a CSV/XLSX/Parquet file into a DataFrame.
    /// Must be called from inside a blocking context (spawn_blocking or std::thread::spawn).
    pub fn load_dataframe(path: &str) -> Result<DataFrame, String> {
        let ext = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let df = match ext.as_str() {
            "csv" => {
                let lf = LazyCsvReader::new(path)
                    .with_has_header(true)
                    .finish()
                    .map_err(|e| format!("Failed to read CSV: {}", e))?;
                lf.collect()
                    .map_err(|e| format!("Failed to collect CSV: {}", e))?
            }
            "parquet" => {
                let lf = LazyFrame::scan_parquet(path.into(), Default::default())
                    .map_err(|e| format!("Failed to scan Parquet: {}", e))?;
                lf.collect()
                    .map_err(|e| format!("Failed to collect Parquet: {}", e))?
            }
            "xlsx" | "xls" => read_xlsx(path)?,
            other => {
                return Err(format!(
                    "Unsupported file type: '.{}'. Supported: csv, xlsx, xls, parquet",
                    other
                ))
            }
        };

        validate_headers(&df)?;

        if df.height() == 0 {
            return Err(
                "File has no data rows — only a header was found".to_string(),
            );
        }

        Ok(df)
    }

    /// Return column names + dtype strings for schema preview.
    pub fn describe_schema(path: &str) -> Result<Vec<(String, String)>, String> {
        let df = Self::load_dataframe(path)?;
        Ok(df
            .schema()
            .iter()
            .map(|(name, dt)| (name.to_string(), dtype_str(dt)))
            .collect())
    }
}
