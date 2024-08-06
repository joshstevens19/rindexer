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

    pub async fn send_message(
        &self,
        channel_id: ChannelId,
        message: &str,
    ) -> Result<(), DiscordError> {
        channel_id.say(&self.http, message).await?;
        Ok(())
    }
}
