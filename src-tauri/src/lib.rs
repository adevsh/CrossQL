pub mod connectors;
pub mod engine;
pub mod writer;
pub mod commands;
pub mod run_manager;
pub mod pipeline_io;
pub mod parquet_query;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .manage(std::sync::Arc::new(run_manager::RunManager::default()))
        .invoke_handler(tauri::generate_handler![
            commands::greet,
            commands::get_process_usage,
            commands::run_pipeline,
            commands::start_pipeline_run,
            commands::cancel_pipeline_run,
            commands::await_pipeline_run,
            commands::preview_pipeline_node,
            commands::preview_postgres_schema,
            commands::preview_mysql_schema,
            commands::preview_mongodb_schema,
            commands::preview_cassandra_schema,
            pipeline_io::save_pipeline,
            pipeline_io::load_pipeline,
            parquet_query::query_parquet
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
