use reqwest::Client;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TwilioError {
    #[error("HTTP request error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("Twilio API error: {0}")]
    ApiError(String),
}

#[derive(Debug, Clone)]
pub struct TwilioBot {
    client: Client,
    account_sid: String,
    auth_token: String,
    from_number: String,
}

impl TwilioBot {
    pub fn new(account_sid: String, auth_token: String, from_number: String) -> Self {
        let client = Client::new();
        Self { client, account_sid, auth_token, from_number }
    }

    pub async fn send_message(&self, to_number: &str, message: &str) -> Result<(), TwilioError> {
        let url = format!(
            "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json",
            self.account_sid
        );

        let response = self
            .client
            .post(&url)
            .basic_auth(&self.account_sid, Some(&self.auth_token))
            .form(&[("To", to_number), ("From", &self.from_number), ("Body", message)])
            .send()
            .await?;

        let status = response.status();
        if status.is_success() || status.as_u16() == 201 {
            Ok(())
        } else {
            let response_text = response.text().await.unwrap_or_default();
            Err(TwilioError::ApiError(format!("HTTP {}: {}", status, response_text)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test that sends a real SMS via Twilio.
    /// Requires env vars: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER, TWILIO_TO_NUMBER
    ///
    /// Run with: cargo test -p rindexer twilio_send_sms -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn twilio_send_sms() {
        let account_sid =
            std::env::var("TWILIO_ACCOUNT_SID").expect("TWILIO_ACCOUNT_SID must be set");
        let auth_token =
            std::env::var("TWILIO_AUTH_TOKEN").expect("TWILIO_AUTH_TOKEN must be set");
        let from_number =
            std::env::var("TWILIO_FROM_NUMBER").expect("TWILIO_FROM_NUMBER must be set");
        let to_number = std::env::var("TWILIO_TO_NUMBER").expect("TWILIO_TO_NUMBER must be set");

        let bot = TwilioBot::new(account_sid, auth_token, from_number);
        bot.send_message(&to_number, "Hello from rindexer smoke test!")
            .await
            .expect("Failed to send SMS");
    }
}
