use polars::prelude::{Column as PolarsColumn, *};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Column, Executor, Row, TypeInfo};

pub struct PostgresConnector;

impl PostgresConnector {
    pub async fn fetch_dataframe(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        query: &str,
    ) -> Result<DataFrame, String> {
        let connect_options = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(database);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await
            .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

        let rows = sqlx::query(query)
            .fetch_all(&pool)
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        if rows.is_empty() {
            return Err("Query returned no rows".to_string());
        }

        // Basic implementation: Convert SQLx rows to Polars DataFrame
        // For Phase 2, we'll start with a simple implementation that handles basic types
        // In a real production scenario, this would be more robust using Arrow integration
        
        let first_row = &rows[0];
        let mut series_vec: Vec<Series> = Vec::new();

        for col in first_row.columns() {
            let col_name = col.name();
            let type_info = col.type_info();
            let type_name = type_info.name();

            // Simplified type handling for Phase 2 proof-of-concept
            match type_name {
                "INT4" => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<Option<i32>, _>(col_name)
                                .ok()
                                .flatten()
                                .map(i64::from)
                        })
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "INT8" => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<i64>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "VARCHAR" | "TEXT" | "BPCHAR" => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<String>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "FLOAT4" => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            row.try_get::<Option<f32>, _>(col_name)
                                .ok()
                                .flatten()
                                .map(f64::from)
                        })
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "FLOAT8" => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<f64>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                 "BOOL" => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<bool>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                _ => {
                     // Fallback to string for unknown types
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                             // This is a simplification; robust mapping needed later
                             row.try_get::<Option<String>, _>(col_name).ok().flatten()
                        })
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
            }
        }

        let columns: Vec<PolarsColumn> = series_vec
            .into_iter()
            .map(PolarsColumn::from)
            .collect();

        let height = if columns.is_empty() { 0 } else { columns[0].len() };
        DataFrame::new(height, columns).map_err(|e| format!("Failed to create DataFrame: {}", e))
    }

    pub async fn describe_schema(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        query: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let connect_options = PgConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(database);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await
            .map_err(|e| format!("Failed to connect to Postgres: {}", e))?;

        let described = (&pool)
            .describe(query)
            .await
            .map_err(|e| format!("Failed to describe query: {}", e))?;

        let mut out = Vec::with_capacity(described.columns().len());
        for c in described.columns() {
            let sql_type = c.type_info().name().to_uppercase();
            let polars_type = match sql_type.as_str() {
                "INT2" | "INT4" | "INT8" => "Int64",
                "FLOAT4" | "FLOAT8" | "NUMERIC" => "Float64",
                "BOOL" => "Boolean",
                "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" | "TIME" => "Datetime",
                "UUID" | "VARCHAR" | "TEXT" | "BPCHAR" => "Utf8",
                _ => "Utf8",
            };
            out.push((c.name().to_string(), polars_type.to_string()));
        }

        Ok(out)
    }
}
