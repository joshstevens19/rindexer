use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_sns::{
    config::http::HttpResponse,
    error::SdkError,
    operation::publish::{PublishError, PublishOutput},
    Client,
};

#[derive(Debug, Clone)]
pub struct SNS {
    client: Client,
}

impl SNS {
    pub async fn new() -> Self {
        let region_provider = RegionProviderChain::default_provider();
        let config =
            aws_config::defaults(BehaviorVersion::latest()).region(region_provider).load().await;
        let client = Client::new(&config);

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
