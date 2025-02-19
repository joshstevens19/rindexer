use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_sns::{
    config::{http::HttpResponse, Credentials},
    error::SdkError,
    operation::publish::{PublishError, PublishOutput},
    Client,
};
use tracing::{error, info};

use crate::types::aws_config::AwsConfig;

#[derive(Debug, Clone)]
#[allow(clippy::upper_case_acronyms)]
pub struct SNS {
    client: Client,
}

impl SNS {
    pub async fn new(config: &AwsConfig) -> Self {
        let region_provider = RegionProviderChain::first_try(Region::new(config.region.clone()));

        let credentials_provider = Credentials::new(
            &config.access_key,
            &config.secret_key,
            config.session_token.clone(),
            None,
            "manual",
        );

        let mut sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .credentials_provider(credentials_provider)
            .load()
            .await;

        // Conditionally set endpoint if it exists
        if let Some(endpoint_url) = &config.endpoint_url {
            if !endpoint_url.trim().is_empty() {
                sdk_config = sdk_config.to_builder()
                    .endpoint_url(endpoint_url)
                    .build();
            }
        }

        let client = Client::new(&sdk_config);

        // Test the connection by listing SNS topics
        match client.list_topics().send().await {
            Ok(_) => {
                info!("Successfully connected to SNS.");
            }
            Err(error) => {
                error!("Error connecting to SNS: {}", error);
                panic!("Error connecting to SNS: {}", error);
            }
        }

        Self { client }
    }

    pub async fn publish(
        &self,
        id: &str,
        topic_arn: &str,
        message: &str,
    ) -> Result<PublishOutput, SdkError<PublishError, HttpResponse>> {
        if topic_arn.contains(".fifo") {
            let result = self
                .client
                .publish()
                .message(message)
                .topic_arn(topic_arn)
                // fifo needs to have group id and deduplication id
                .message_group_id("default")
                .message_deduplication_id(id)
                .send()
                .await?;

            Ok(result)
        } else {
            let result = self.client.publish().topic_arn(topic_arn).message(message).send().await?;
            Ok(result)
        }
    }
}
