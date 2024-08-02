use std::sync::Arc;

use aws_sdk_sns::{config::http::HttpResponse, error::SdkError, operation::publish::PublishError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{task, task::JoinError};
use tracing::error;

use crate::{
    manifest::stream::{SNSStreamConfig, StreamsConfig},
    streams::SNS,
};

// we should limit the max chunk size we send over when streaming to 100KB
const MAX_CHUNK_SIZE: usize = 100 * 1024; // 100 KB

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventMessage {
    pub event_name: String,
    pub event_data: Value,
    pub network: String,
}

#[derive(Debug, Clone)]
struct SNSStream {
    config: Vec<SNSStreamConfig>,
    client: Arc<SNS>,
}

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("SNS could not public - {0}")]
    SnsCouldNotPublish(#[from] SdkError<PublishError, HttpResponse>),

    #[error("SNS Task failed: {0}")]
    SnsJoinError(JoinError),
}

pub struct StreamsClients {
    sns: Option<SNSStream>,
}

impl StreamsClients {
    pub async fn new(stream_config: StreamsConfig) -> Self {
        let sns = if let Some(config) = &stream_config.sns {
            Some(SNSStream { config: config.clone(), client: Arc::new(SNS::new().await) })
        } else {
            None
        };

        Self { sns }
    }

    fn chunk_data(&self, data_array: &Vec<Value>) -> Vec<Vec<Value>> {
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        let mut chunks = Vec::new();
        for item in data_array {
            let item_str = serde_json::to_string(item).unwrap();
            let item_size = item_str.len();

            if current_size + item_size > MAX_CHUNK_SIZE {
                chunks.push(current_chunk);
                current_chunk = Vec::new();
                current_size = 0;
            }

            current_chunk.push(item.clone());
            current_size += item_size;
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        chunks
    }

    pub async fn stream(
        &self,
        id: String,
        event_message: EventMessage,
    ) -> Result<usize, StreamError> {
        if let Value::Array(data_array) = &event_message.event_data {
            if let Some(sns) = &self.sns {
                for config in &sns.config {
                    if config.events.contains(&event_message.event_name) &&
                        config.networks.contains(&event_message.network)
                    {
                        let chunks = self.chunk_data(data_array);
                        let total_streamed: usize = chunks.iter().map(|chunk| chunk.len()).sum();

                        let tasks: Vec<_> = chunks
                            .into_iter()
                            .enumerate()
                            .map(|(index, chunk)| {
                                let prefix_id = config
                                    .prefix_id
                                    .clone()
                                    .unwrap_or("rindexer_stream".to_string());
                                let id =
                                    format!("{}__{}-clunk-{}", prefix_id, id.to_lowercase(), index);
                                let client = sns.client.clone();
                                let topic_arn = config.topic_arn.clone();
                                let chunk_message = EventMessage {
                                    event_name: event_message.event_name.clone(),
                                    event_data: Value::Array(chunk.to_vec()),
                                    network: event_message.network.clone(),
                                };
                                let message_str = serde_json::to_string(&chunk_message).unwrap();
                                task::spawn(async move {
                                    client
                                        .publish(&id, &topic_arn, &message_str)
                                        .await
                                        .map_err(StreamError::SnsCouldNotPublish)
                                })
                            })
                            .collect();

                        for task in tasks {
                            match task.await {
                                Ok(Ok(_)) => (),
                                Ok(Err(e)) => return Err(e),
                                Err(e) => return Err(StreamError::SnsJoinError(e)),
                            }
                        }

                        return Ok(total_streamed);
                    }
                }
            };

            Ok(0)
        } else {
            unreachable!("Event data should be an array");
        }
    }
}
