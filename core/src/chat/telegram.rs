use teloxide::{prelude::*, types::ParseMode, RequestError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TelegramError {
    #[error("Telegram API error: {0}")]
    ApiError(#[from] RequestError),
}

#[derive(Debug, Clone)]
pub struct TelegramBot {
    bot: Bot,
}

impl TelegramBot {
    pub fn new(token: &str) -> Self {
        let bot = Bot::new(token);
        Self { bot }
    }

    pub async fn send_message(&self, chat_id: ChatId, message: &str) -> Result<(), TelegramError> {
        self.bot.send_message(chat_id, message).parse_mode(ParseMode::MarkdownV2).await?;
        Ok(())
    }
}
