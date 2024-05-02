use csv::Writer;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Mutex;

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

    pub async fn append(&self, data: Vec<String>) -> Result<(), csv::Error> {
        let lock = self.writer_lock.clone();
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock();
            let file = File::options().create(true).append(true).open(&path)?;
            let mut writer = Writer::from_writer(file);

            writer.write_record(data)?;

            Ok(())
        })
        .await
        .expect("Failed to run CSV write operation")
    }

    pub async fn append_header(&self, header: Vec<String>) -> Result<(), csv::Error> {
        let lock = self.writer_lock.clone();
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock();
            let file = File::options().create(true).append(true).open(&path)?;
            let mut writer = Writer::from_writer(file);

            writer.write_record(header)?;

            Ok(())
        })
        .await
        .expect("Failed to run CSV write operation")
    }
}
