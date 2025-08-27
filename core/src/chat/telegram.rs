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
        let escaped_message = self.escape_markdown_v2(message);
        self.bot.send_message(chat_id, &escaped_message)
            .parse_mode(ParseMode::MarkdownV2)
            .await?;
        Ok(())
    }

    fn escape_markdown_v2(&self, text: &str) -> String {
        text.chars()
            .map(|c| match c {
                '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|' | '{' | '}' | '.' | '!' | '\\' => {
                    format!("\\{}", c)
                }
                _ => c.to_string(),
            })
            .collect()
    }
}
