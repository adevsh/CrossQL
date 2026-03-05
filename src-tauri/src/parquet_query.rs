use polars::prelude::*;
use polars::sql::SQLContext;

#[derive(serde::Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_rows: usize,
}

/// Query a Parquet file using SQL via Polars SQLContext.
/// The file is registered as a table named "result".
/// Example: SELECT * FROM result WHERE city = 'Solo' LIMIT 50
///
/// Polars `collect()` internally creates its own Tokio runtime, which panics
/// if called from within the existing Tauri Tokio runtime. We use
/// `std::thread::spawn` to escape the runtime context.
#[tauri::command]
pub async fn query_parquet(path: String, sql: String) -> Result<QueryResult, String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<Result<QueryResult, String>>();

    std::thread::spawn(move || {
        let result = (|| -> Result<QueryResult, String> {
            let lf = LazyFrame::scan_parquet(
                path.as_str().into(),
                Default::default(),
            )
            .map_err(|e| format!("Failed to read parquet: {}", e))?;

            let mut ctx = SQLContext::new();
            ctx.register("result", lf);

            let queried_lf = ctx
                .execute(&sql)
                .map_err(|e| format!("SQL error: {}", e))?;

            let df = queried_lf
                .limit(200)
                .collect()
                .map_err(|e| format!("Failed to collect: {}", e))?;

            let columns: Vec<String> = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let total_rows = df.height();

            let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
            for i in 0..total_rows {
                let mut row: Vec<serde_json::Value> = Vec::new();
                for (ci, _) in columns.iter().enumerate() {
                    let s = df.select_at_idx(ci).unwrap();
                    let val = format!("{}", s.get(i).unwrap());
                    let json_val = if val == "null" {
                        serde_json::Value::Null
                    } else if let Ok(n) = val.parse::<i64>() {
                        serde_json::json!(n)
                    } else if let Ok(f) = val.parse::<f64>() {
                        serde_json::json!(f)
                    } else if val == "true" {
                        serde_json::json!(true)
                    } else if val == "false" {
                        serde_json::json!(false)
                    } else {
                        let clean = val.trim_matches('"').to_string();
                        serde_json::json!(clean)
                    };
                    row.push(json_val);
                }
                rows.push(row);
            }

            Ok(QueryResult {
                columns,
                rows,
                total_rows,
            })
        })();
        let _ = tx.send(result);
    });

    rx.await.map_err(|_| "Query thread panicked".to_string())?
}
