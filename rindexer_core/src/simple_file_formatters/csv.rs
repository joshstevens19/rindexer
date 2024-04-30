use tokio::sync::Mutex;
use std::sync::Arc;
use serde::Serialize;
use csv::Writer;
use std::fs::File;

#[derive(Debug)]
pub struct AsyncCsvAppender {
    path: String,
    writer_lock: Arc<Mutex<()>>,
}

impl AsyncCsvAppender {
    pub fn new(file_path: String) -> Self {
        AsyncCsvAppender {
            path: file_path,
            writer_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn append<T: Serialize + Send + 'static>(&self, data: T) -> Result<(), csv::Error> {
        let path = self.path.clone();
        let lock = self.writer_lock.clone();

        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock();

            let file = File::options().create(true).append(true).open(&path)?;
            let mut writer = Writer::from_writer(file);

            writer.serialize(data)?;
            writer.flush()?;

            Ok::<(), csv::Error>(())
        }).await.expect("Failed to run CSV write operation")
    }
}
