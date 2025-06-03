use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use csv::Writer;
use csv::Reader;
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
            // Create parent directories if they don't exist
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("Failed to create directory");
            }
            let file = File::options().create(true).append(true).open(&path)?;
            let mut writer = Writer::from_writer(file);

            writer.write_record(header)?;

            Ok(())
        })
        .await
        .expect("Failed to run CSV write operation")
    }
}

pub struct AsyncCsvReader {
    path: Arc<Path>,
}

impl AsyncCsvReader {
    pub fn new(file_path: &str) -> Self {
        AsyncCsvReader { path: Arc::from(PathBuf::from(file_path)) }
    }

    pub async fn read_all(&self) -> Result<Vec<Vec<String>>, csv::Error> {
        let path = Arc::clone(&self.path);

        tokio::task::spawn_blocking(move || {
            let file = File::open(&path)?;
            let mut reader = Reader::from_reader(file);

            let mut records = Vec::new();
            for result in reader.records() {
                let record = result?;
                records.push(record.iter().map(|s| s.to_string()).collect());
            }

            Ok(records)
        })
        .await
        .expect("Failed to run CSV read operation")
    }
}
