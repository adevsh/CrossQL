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
        {
            let mut r = self.result.lock().await;
            *r = Some(res);
        }
        // notify_one stores a permit if nobody is waiting yet,
        // so the next notified().await returns immediately.
        self.notify.notify_one();
    }

    pub async fn wait_result(&self) -> Result<serde_json::Value, String> {
        loop {
            // Register the notification BEFORE checking the result
            // to avoid lost notifications between check and await.
            let notified = self.notify.notified();
            {
                let lock = self.result.lock().await;
                if let Some(res) = lock.as_ref() {
                    return res.clone();
                }
            }
            notified.await;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn await_run_returns_result_and_removes_entry() {
        let manager = RunManager::default();
        let (run_id, entry) = manager.create_run().await;
        entry.set_result(Ok(serde_json::json!({ "ok": true }))).await;

        let result = manager.await_run(&run_id).await.unwrap();
        assert_eq!(result["ok"], true);

        let missing = manager.await_run(&run_id).await;
        assert!(missing.is_err());
    }

    #[tokio::test]
    async fn cancel_run_cancels_token() {
        let manager = RunManager::default();
        let (run_id, entry) = manager.create_run().await;

        let cancelled = manager.cancel_run(&run_id).await;
        assert!(cancelled);
        assert!(entry.cancel.is_cancelled());
    }

    #[tokio::test]
    async fn finish_run_removes_without_await() {
        let manager = RunManager::default();
        let (run_id, _) = manager.create_run().await;

        manager.finish_run(&run_id).await;

        let missing = manager.await_run(&run_id).await;
        assert!(missing.is_err());
    }
}
