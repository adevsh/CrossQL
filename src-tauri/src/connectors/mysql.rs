use polars::prelude::{Column as PolarsColumn, *};
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{Column, Executor, Row, TypeInfo};

pub struct MysqlConnector;

impl MysqlConnector {
    pub async fn fetch_dataframe(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        query: &str,
    ) -> Result<DataFrame, String> {
        let connect_options = MySqlConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(database);

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await
            .map_err(|e| format!("Failed to connect to MySQL: {}", e))?;

        let rows = sqlx::query(query)
            .fetch_all(&pool)
            .await
            .map_err(|e| format!("Failed to execute query: {}", e))?;

        if rows.is_empty() {
            return Err("Query returned no rows".to_string());
        }

        let first_row = &rows[0];
        let mut series_vec: Vec<Series> = Vec::new();

        for col in first_row.columns() {
            let col_name = col.name();
            let type_name = col.type_info().name().to_uppercase();

            match type_name.as_str() {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" => {
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
                "BIGINT" => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<i64>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "FLOAT" => {
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
                "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<f64>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<String>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                "BIT" | "BOOL" | "BOOLEAN" => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<bool>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
                _ => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| row.try_get::<Option<String>, _>(col_name).ok().flatten())
                        .collect();
                    series_vec.push(Series::new(col_name.into(), values));
                }
            }
        }

        let columns: Vec<PolarsColumn> = series_vec.into_iter().map(PolarsColumn::from).collect();
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
        let connect_options = MySqlConnectOptions::new()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .database(database);

        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await
            .map_err(|e| format!("Failed to connect to MySQL: {}", e))?;

        let described = (&pool)
            .describe(query)
            .await
            .map_err(|e| format!("Failed to describe query: {}", e))?;

        let mut out = Vec::with_capacity(described.columns().len());
        for c in described.columns() {
            let sql_type = c.type_info().name().to_uppercase();
            let polars_type = match sql_type.as_str() {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" => "Int64",
                "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => "Utf8",
                "DATETIME" | "TIMESTAMP" | "DATE" | "TIME" => "Datetime",
                "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "NUMERIC" => "Float64",
                "BIT" | "BOOL" | "BOOLEAN" => "Boolean",
                _ => "Utf8",
            };
            out.push((c.name().to_string(), polars_type.to_string()));
        }

        Ok(out)
    }
}
