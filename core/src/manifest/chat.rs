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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TwilioConfig {
    pub account_sid: String,
    pub auth_token: String,
    pub from_number: String,
    pub to_number: String,
    pub networks: Vec<String>,
    pub messages: Vec<TwilioEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TwilioEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PagerDutyConfig {
    pub routing_key: String,
    #[serde(default = "default_pagerduty_severity")]
    pub severity: String,
    pub networks: Vec<String>,
    pub messages: Vec<PagerDutyEvent>,
}

fn default_pagerduty_severity() -> String {
    "critical".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PagerDutyEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

    pub template_inline: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpsGenieConfig {
    pub api_key: String,
    #[serde(default = "default_opsgenie_priority")]
    pub priority: String,
    pub networks: Vec<String>,
    pub messages: Vec<OpsGenieEvent>,
}

fn default_opsgenie_priority() -> String {
    "P1".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpsGenieEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_expression: Option<String>,

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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub twilio: Option<Vec<TwilioConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagerduty: Option<Vec<PagerDutyConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opsgenie: Option<Vec<OpsGenieConfig>>,
}
