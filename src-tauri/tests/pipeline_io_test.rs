use crossql_lib::pipeline_io::{load_pipeline, save_pipeline};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_path(name: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("crossql_{}_{}.etl.json", name, stamp))
}

#[doc = "Expected: save_pipeline writes data and load_pipeline reads back the same content.\nBehavior: file is created on disk and loaded value matches the original JSON payload."]
#[tokio::test]
async fn save_and_load_pipeline_roundtrip() {
    let path = unique_path("io_roundtrip");
    let payload = r#"{"name":"test","nodes":[],"edges":[]}"#.to_string();

    save_pipeline(path.to_string_lossy().to_string(), payload.clone())
        .await
        .unwrap();
    let loaded = load_pipeline(path.to_string_lossy().to_string()).await.unwrap();

    assert_eq!(loaded, payload);
    let _ = fs::remove_file(path);
}

#[doc = "Expected: load_pipeline returns an error for missing files.\nBehavior: function fails with a descriptive read error when path does not exist."]
#[tokio::test]
async fn load_pipeline_returns_error_for_missing_file() {
    let path = unique_path("io_missing");
    let result = load_pipeline(path.to_string_lossy().to_string()).await;

    match result {
        Err(e) => assert!(e.starts_with("Failed to read file:")),
        Ok(_) => panic!("expected load_pipeline to fail for missing file"),
    }
}
