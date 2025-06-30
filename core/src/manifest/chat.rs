use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[cfg(feature = "telegram")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: i64,
    pub networks: Vec<String>,
    pub messages: Vec<TelegramEvent>,
}

#[cfg(feature = "telegram")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelegramEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[cfg(feature = "discord")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiscordConfig {
    pub bot_token: String,
    pub channel_id: u64,
    pub networks: Vec<String>,
    pub messages: Vec<DiscordEvent>,
}

#[cfg(feature = "discord")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiscordEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[cfg(feature = "slack")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SlackConfig {
    pub bot_token: String,
    pub channel: String,
    pub networks: Vec<String>,
    pub messages: Vec<SlackEvent>,
}

#[cfg(feature = "slack")]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SlackEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatConfig {
    #[cfg(feature = "telegram")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<Vec<TelegramConfig>>,

    #[cfg(feature = "discord")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discord: Option<Vec<DiscordConfig>>,

    #[cfg(feature = "slack")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<Vec<SlackConfig>>,
}
