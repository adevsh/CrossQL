use crossql_lib::parquet_query::query_parquet;
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

#[doc = "Expected: valid SQL query over parquet returns selected columns and filtered rows.\nBehavior: query_parquet reads fixture parquet, executes SQL, and returns deterministic columns, rows, and total_rows."]
#[tokio::test]
async fn query_parquet_returns_rows_and_columns_for_valid_sql() {
    let df = df!(
        "id" => &[1_i64, 2, 3],
        "city" => &["Solo", "Tokyo", "Seoul"],
        "active" => &[true, false, true]
    )
    .unwrap();
    let path = unique_parquet_path("query_ok");
    CrossQLParquetWriter::write_dataframe(
        df,
        path.to_string_lossy().as_ref(),
        "snappy",
        None,
    )
    .unwrap();

    let result = query_parquet(
        path.to_string_lossy().to_string(),
        "SELECT id, city FROM result WHERE active = true ORDER BY id".to_string(),
    )
    .await
    .unwrap();

    assert_eq!(result.columns, vec!["id".to_string(), "city".to_string()]);
    assert_eq!(result.total_rows, 2);
    assert_eq!(
        result.rows,
        vec![
            vec![serde_json::json!(1), serde_json::json!("Solo")],
            vec![serde_json::json!(3), serde_json::json!("Seoul")]
        ]
    );

    let _ = fs::remove_file(path);
}

#[doc = "Expected: invalid SQL returns a SQL error.\nBehavior: query_parquet fails with SQL error message when query references a missing column."]
#[tokio::test]
async fn query_parquet_returns_error_for_invalid_sql() {
    let df = df!("id" => &[1_i64, 2]).unwrap();
    let path = unique_parquet_path("query_sql_err");
    CrossQLParquetWriter::write_dataframe(
        df,
        path.to_string_lossy().as_ref(),
        "snappy",
        None,
    )
    .unwrap();

    let result = query_parquet(
        path.to_string_lossy().to_string(),
        "SELECT missing_col FROM result".to_string(),
    )
    .await;

    match result {
        Err(e) => assert!(e.starts_with("SQL error:")),
        Ok(_) => panic!("expected query_parquet to fail for invalid SQL"),
    }

    let _ = fs::remove_file(path);
}
