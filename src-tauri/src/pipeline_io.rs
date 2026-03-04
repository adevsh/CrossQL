use std::fs;
use std::path::Path;

/// Save pipeline JSON to a file.
#[tauri::command]
pub async fn save_pipeline(path: String, data: String) -> Result<(), String> {
    if let Some(parent) = Path::new(&path).parent() {
        fs::create_dir_all(parent).map_err(|e| format!("Failed to create directory: {}", e))?;
    }
    fs::write(&path, &data).map_err(|e| format!("Failed to write file: {}", e))?;
    Ok(())
}

/// Load pipeline JSON from a file.
#[tauri::command]
pub async fn load_pipeline(path: String) -> Result<String, String> {
    fs::read_to_string(&path).map_err(|e| format!("Failed to read file: {}", e))
}
