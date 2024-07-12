use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use csv::Writer;
use tokio::sync::Mutex;

pub struct AsyncCsvAppender {
    path: Arc<Path>,
    writer_lock: Arc<Mutex<()>>,
}

impl AsyncCsvAppender {
    pub fn new(file_path: &str) -> Self {
        AsyncCsvAppender {
            path: Arc::from(PathBuf::from(file_path)),
            writer_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn append(&self, data: Vec<String>) -> Result<(), csv::Error> {
        let lock = Arc::clone(&self.writer_lock);
        let path = Arc::clone(&self.path);

        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock();
            let file = File::options().create(true).append(true).open(path)?;
            let mut writer = Writer::from_writer(file);

            writer.write_record(data)?;

            Ok(())
        })
        .await
        .expect("Failed to run CSV write operation")
    }

    pub async fn append_bulk(&self, records: Vec<Vec<String>>) -> Result<(), csv::Error> {
        let lock = Arc::clone(&self.writer_lock);
        let path = Arc::clone(&self.path);

        tokio::task::spawn_blocking(move || {
            let _guard = lock.lock();
            let file = File::options().create(true).append(true).open(&path)?;
            let mut writer = Writer::from_writer(file);

            for record in records {
                writer.write_record(record)?;
            }

            Ok(())
        })
        .await
        .expect("Failed to run CSV bulk write operation")
    }

    pub async fn append_header(&self, header: Vec<String>) -> Result<(), csv::Error> {
        let lock = Arc::clone(&self.writer_lock);
        let path = Arc::clone(&self.path);

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
