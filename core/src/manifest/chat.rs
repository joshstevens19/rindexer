use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: i64,
    pub networks: Vec<String>,
    pub messages: Vec<TelegramEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelegramEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiscordConfig {
    pub bot_token: String,
    pub channel_id: u64,
    pub networks: Vec<String>,
    pub messages: Vec<DiscordEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiscordEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SlackConfig {
    pub bot_token: String,
    pub channel: String,
    pub networks: Vec<String>,
    pub messages: Vec<SlackEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SlackEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChatConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub telegram: Option<Vec<TelegramConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discord: Option<Vec<DiscordConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<Vec<SlackConfig>>,
}
