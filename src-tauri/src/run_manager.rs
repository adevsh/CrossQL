use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct RunEntry {
    pub cancel: CancellationToken,
    result: Mutex<Option<Result<serde_json::Value, String>>>,
    notify: Notify,
}

impl RunEntry {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub async fn set_result(&self, res: Result<serde_json::Value, String>) {
        let mut r = self.result.lock().await;
        *r = Some(res);
        self.notify.notify_waiters();
    }

    pub async fn wait_result(&self) -> Result<serde_json::Value, String> {
        loop {
            if let Some(res) = self.result.lock().await.clone() {
                return res;
            }
            self.notify.notified().await;
        }
    }
}

#[derive(Default)]
pub struct RunManager {
    runs: Mutex<HashMap<String, Arc<RunEntry>>>,
}

impl RunManager {
    pub async fn create_run(&self) -> (String, Arc<RunEntry>) {
        let id = Uuid::new_v4().to_string();
        let entry = Arc::new(RunEntry::new(CancellationToken::new()));
        let mut runs = self.runs.lock().await;
        runs.insert(id.clone(), entry.clone());
        (id, entry)
    }

    pub async fn cancel_run(&self, run_id: &str) -> bool {
        let runs = self.runs.lock().await;
        if let Some(entry) = runs.get(run_id) {
            entry.cancel.cancel();
            return true;
        }
        false
    }

    pub async fn await_run(&self, run_id: &str) -> Result<serde_json::Value, String> {
        let entry = {
            let runs = self.runs.lock().await;
            runs.get(run_id).cloned()
        };
        let entry = entry.ok_or_else(|| "Run not found".to_string())?;
        let res = entry.wait_result().await;
        let mut runs = self.runs.lock().await;
        runs.remove(run_id);
        res
    }

    pub async fn finish_run(&self, run_id: &str) {
        let mut runs = self.runs.lock().await;
        runs.remove(run_id);
    }
}
