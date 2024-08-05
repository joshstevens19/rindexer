use std::sync::Arc;

use ethers::types::U64;
use serde_json::{Map, Value};
use teloxide::types::ChatId;
use thiserror::Error;
use tokio::{
    task,
    task::{JoinError, JoinHandle},
};

use crate::{
    chat::{
        telegram::{TelegramBot, TelegramError},
        template::Template,
    },
    event::EventMessage,
    manifest::chat::{ChatConfig, TelegramConfig, TelegramEvent},
};

type SendMessage = Vec<JoinHandle<Result<(), ChatError>>>;

#[derive(Error, Debug)]
pub enum ChatError {
    #[error("Telegram error: {0}")]
    TelegramError(#[from] TelegramError),

    #[error("Task failed: {0}")]
    JoinError(JoinError),
}

#[derive(Debug, Clone)]
struct TelegramInstance {
    config: TelegramConfig,
    client: Arc<TelegramBot>,
}

pub struct ChatClients {
    telegram: Option<Vec<TelegramInstance>>,
}

impl ChatClients {
    pub async fn new(chat_config: ChatConfig) -> Self {
        let telegram = chat_config.telegram.map(|config| {
            config
                .into_iter()
                .map(|config| {
                    let client = Arc::new(TelegramBot::new(&config.bot_token));
                    TelegramInstance { config, client }
                })
                .collect()
        });

        Self { telegram }
    }

    fn find_accepted_block_range(&self, from_block: &U64, to_block: &U64) -> U64 {
        if from_block > to_block {
            panic!("Invalid range: from_block must be less than or equal to to_block");
        }

        match from_block.overflowing_add(to_block - from_block) {
            (result, false) => result,
            (_, true) => U64::max_value(),
        }
    }

    pub fn is_in_block_range_to_send(&self, from_block: &U64, to_block: &U64) -> bool {
        // only 10 blocks at a time else rate limits will kick in
        U64::from(10) <= self.find_accepted_block_range(from_block, to_block)
    }

    fn has_any_chat(&self) -> bool {
        self.telegram.is_some()
    }

    fn get_nested_value(&self, data: &Value, path: &str) -> Option<Value> {
        let keys: Vec<&str> = path.split('.').collect();
        let mut current = data;
        for key in keys {
            match current.get(key) {
                Some(value) => current = value,
                None => return None,
            }
        }
        Some(current.clone())
    }

    #[allow(clippy::manual_strip)]
    fn evaluate_condition(&self, value: &Value, condition: &str) -> bool {
        if condition.contains("||") ||
            condition.contains("&&") ||
            condition.contains('>') ||
            condition.contains('<') ||
            condition.contains('=')
        {
            let parts: Vec<&str> = condition.split("||").collect();
            for part in parts {
                let subparts: Vec<&str> = part.split("&&").collect();
                let mut and_result = true;
                for subpart in subparts {
                    let (op, comp) = if subpart.starts_with(">=") {
                        (">=", &subpart[2..])
                    } else if subpart.starts_with("<=") {
                        ("<=", &subpart[2..])
                    } else if subpart.starts_with(">") {
                        (">", &subpart[1..])
                    } else if subpart.starts_with("<") {
                        ("<", &subpart[1..])
                    } else if subpart.starts_with("=") {
                        ("=", &subpart[1..])
                    } else {
                        ("", subpart)
                    };

                    and_result &= match op {
                        ">=" => {
                            U64::from_str_radix(value.as_str().unwrap_or("0"), 10)
                                .unwrap_or_default() >=
                                U64::from_str_radix(comp, 10).unwrap_or_default()
                        }
                        "<=" => {
                            U64::from_str_radix(value.as_str().unwrap_or("0"), 10)
                                .unwrap_or_default() <=
                                U64::from_str_radix(comp, 10).unwrap_or_default()
                        }
                        ">" => {
                            U64::from_str_radix(value.as_str().unwrap_or("0"), 10)
                                .unwrap_or_default() >
                                U64::from_str_radix(comp, 10).unwrap_or_default()
                        }
                        "<" => {
                            U64::from_str_radix(value.as_str().unwrap_or("0"), 10)
                                .unwrap_or_default() <
                                U64::from_str_radix(comp, 10).unwrap_or_default()
                        }
                        "=" => value == &Value::String(comp.to_string()),
                        "" => value == &Value::String(subpart.to_string()), /* Exact match if no
                                                                              * operator */
                        _ => false,
                    };
                }
                if and_result {
                    return true;
                }
            }
            false
        } else {
            value == &Value::String(condition.to_string())
        }
    }

    fn filter_event(&self, event_data: &Value, conditions: &Vec<Map<String, Value>>) -> bool {
        for condition in conditions {
            for (key, value) in condition {
                if let Some(event_value) = self.get_nested_value(event_data, key) {
                    if !self.evaluate_condition(&event_value, value.as_str().unwrap_or("")) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        true
    }

    fn telegram_send_message_tasks(
        &self,
        instance: &TelegramInstance,
        event_for: &TelegramEvent,
        events_data: &[Value],
    ) -> SendMessage {
        let tasks: Vec<_> = events_data
            .iter()
            .filter(|event_data| {
                if let Some(conditions) = &event_for.conditions {
                    self.filter_event(event_data, conditions)
                } else {
                    true
                }
            })
            .map(|event_data| {
                let client = Arc::clone(&instance.client);
                let chat_id = ChatId(instance.config.chat_id);
                let message = Template::new(event_for.template_inline.clone())
                    .parse_template_inline(event_data);
                task::spawn(async move {
                    client.send_message(chat_id, &message).await?;
                    Ok(())
                })
            })
            .collect();
        tasks
    }

    pub async fn send_message(
        &self,
        event_message: &EventMessage,
        from_block: &U64,
        to_block: &U64,
    ) -> Result<usize, ChatError> {
        if !self.has_any_chat() || !self.is_in_block_range_to_send(from_block, to_block) {
            return Ok(0);
        }

        // will always have something even if the event has no parameters due to the tx_information
        if let Value::Array(data_array) = &event_message.event_data {
            let mut messages: Vec<SendMessage> = Vec::new();

            if let Some(telegram) = &self.telegram {
                for instance in telegram {
                    if instance.config.networks.contains(&event_message.network) {
                        let telegram_event = instance
                            .config
                            .messages
                            .iter()
                            .find(|e| e.event_name == event_message.event_name);

                        if let Some(telegram_event) = telegram_event {
                            let message = self.telegram_send_message_tasks(
                                instance,
                                telegram_event,
                                data_array,
                            );
                            messages.push(message);
                        }
                    }
                }
            }

            let mut messages_sent = 0;

            for message in messages {
                for publish in message {
                    match publish.await {
                        Ok(Ok(_)) => messages_sent += 1,
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(ChatError::JoinError(e)),
                    }
                }
            }

            Ok(messages_sent)
        } else {
            unreachable!("Event data should be an array");
        }
    }
}
