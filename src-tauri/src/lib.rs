pub mod connectors;
pub mod engine;
pub mod writer;
pub mod commands;
pub mod run_manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(std::sync::Arc::new(run_manager::RunManager::default()))
        .invoke_handler(tauri::generate_handler![
            commands::greet,
            commands::get_process_usage,
            commands::run_pipeline,
            commands::start_pipeline_run,
            commands::cancel_pipeline_run,
            commands::await_pipeline_run,
            commands::preview_pipeline_node,
            commands::run_postgres_to_parquet,
            commands::run_mysql_to_parquet,
            commands::preview_postgres_schema,
            commands::preview_mysql_schema,
            commands::run_mongodb_to_parquet,
            commands::preview_mongodb_schema,
            commands::run_cassandra_to_parquet,
            commands::preview_cassandra_schema
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
