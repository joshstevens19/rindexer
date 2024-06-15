use csv::Writer;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A structure for appending records to a CSV file asynchronously.
pub struct AsyncCsvAppender {
    path: String,
    writer_lock: Arc<Mutex<()>>,
}

impl AsyncCsvAppender {
    /// Creates a new `AsyncCsvAppender`.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the CSV file.
    ///
    /// # Returns
    ///
    /// An instance of `AsyncCsvAppender`.
    pub fn new(file_path: String) -> Self {
        AsyncCsvAppender {
            path: file_path,
            writer_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Appends a record to the CSV file.
    ///
    /// # Arguments
    ///
    /// * `data` - A vector of strings representing the record to append.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
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

    /// Appends a header to the CSV file.
    ///
    /// # Arguments
    ///
    /// * `header` - A vector of strings representing the header to append.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
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
