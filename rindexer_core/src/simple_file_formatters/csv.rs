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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_append() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();
        let appender = AsyncCsvAppender::new(file_path.clone());

        let data = vec!["field1".to_string(), "field2".to_string()];
        appender.append(data).await.unwrap();

        let contents = fs::read_to_string(file_path).unwrap();
        assert!(contents.contains("field1,field2"));
    }

    #[tokio::test]
    async fn test_append_header() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();
        let appender = AsyncCsvAppender::new(file_path.clone());

        let header = vec!["header1".to_string(), "header2".to_string()];
        appender.append_header(header).await.unwrap();

        let contents = fs::read_to_string(file_path).unwrap();
        assert!(contents.contains("header1,header2"));
    }

    #[tokio::test]
    async fn test_append_and_append_header() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap().to_string();
        let appender = AsyncCsvAppender::new(file_path.clone());

        let header = vec!["header1".to_string(), "header2".to_string()];
        appender.append_header(header).await.unwrap();

        let data = vec!["field1".to_string(), "field2".to_string()];
        appender.append(data).await.unwrap();

        let contents = fs::read_to_string(file_path).unwrap();
        assert!(contents.contains("header1,header2"));
        assert!(contents.contains("field1,field2"));
    }
}
