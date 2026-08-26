use serenity::{http::Http, model::id::ChannelId};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiscordError {
    #[error("Discord API error: {0}")]
    ApiError(#[from] serenity::Error),
}

#[derive(Debug)]
pub struct DiscordBot {
    http: Http,
}

impl DiscordBot {
    pub fn new(token: &str) -> Self {
        let http = Http::new(token);
        Self { http }
    }

    #[allow(clippy::result_large_err, reason = "preserve existing error API")]
    pub async fn send_message(
        &self,
        channel_id: ChannelId,
        message: &str,
    ) -> Result<(), DiscordError> {
        channel_id.say(&self.http, message).await?;
        Ok(())
    }
}
