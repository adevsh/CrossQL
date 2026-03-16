//! Integration tests for the PostgreSQL executor.
//! Prerequisites: run `make docker-up` before executing these tests.
//! These tests require CROSSQL_SRC_POSTGRES_URL and reachable PostgreSQL.

use chrono::Utc;
use crossql_engine::executor::sink::{sink_parquet, SinkCompression};
use polars::lazy::prelude::IntoLazy;
use polars::prelude::{DataFrame, NamedFrom, Series};
use sqlx::Row;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn test_full_pipeline_postgres_to_parquet() {
    let url = std::env::var("CROSSQL_SRC_POSTGRES_URL").expect("CROSSQL_SRC_POSTGRES_URL is required");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();

    sqlx::query("CREATE TABLE IF NOT EXISTS crossql_phase3_pg (id BIGINT, value TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO crossql_phase3_pg (id, value) VALUES (1, 'a'), (2, 'b')")
        .execute(&pool)
        .await
        .unwrap();

    let rows = sqlx::query("SELECT id, value FROM crossql_phase3_pg ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();

    let ids: Vec<i64> = rows.iter().map(|r| r.get::<i64, _>("id")).collect();
    let values: Vec<String> = rows.iter().map(|r| r.get::<String, _>("value")).collect();
    let df = DataFrame::new(2, vec![
        Series::new("id".into(), ids).into(),
        Series::new("value".into(), values).into(),
    ])
    .unwrap();

    let out_root = std::env::temp_dir().join(format!("crossql-engine-it-pg-{}", Uuid::new_v4()));
    let out = sink_parquet(
        df.lazy(),
        &out_root,
        "pg_pipeline",
        1,
        "job-it",
        Utc::now(),
        SinkCompression::Snappy,
    )
    .unwrap();
    assert!(out.exists());
}
