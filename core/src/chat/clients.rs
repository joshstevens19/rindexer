use alloy::primitives::U64;
use futures::future::join_all;
use serde_json::Value;
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};

#[cfg(any(feature = "discord", feature = "telegram", feature = "slack"))]
use std::sync::Arc;
#[cfg(any(feature = "discord", feature = "telegram", feature = "slack"))]
use tokio::task;

#[cfg(feature = "discord")]
use crate::{
    chat::discord::{DiscordBot, DiscordError},
    manifest::chat::{DiscordConfig, DiscordEvent},
};
#[cfg(feature = "discord")]
use serenity::all::ChannelId;

#[cfg(feature = "slack")]
use crate::{
    chat::slack::{SlackBot, SlackError},
    manifest::chat::{SlackConfig, SlackEvent},
};

#[cfg(feature = "telegram")]
use crate::{
    chat::{
        telegram::{TelegramBot, TelegramError},
        template::Template,
    },
    manifest::chat::{TelegramConfig, TelegramEvent},
};
#[cfg(feature = "telegram")]
use teloxide::types::ChatId;

use crate::event::{filter_event_data_by_conditions, EventMessage};
use crate::manifest::chat::ChatConfig;

type SendMessage = Vec<JoinHandle<Result<(), ChatError>>>;

#[derive(Error, Debug)]
pub enum ChatError {
    #[cfg(feature = "telegram")]
    #[error("Telegram error: {0}")]
    Telegram(#[from] TelegramError),

    #[cfg(feature = "discord")]
    #[error("Discord error: {0}")]
    Discord(#[from] DiscordError),

    #[cfg(feature = "slack")]
    #[error("Slack error: {0}")]
    Slack(#[from] SlackError),

    #[error("Task failed: {0}")]
    JoinError(JoinError),
}

#[cfg(feature = "telegram")]
#[derive(Debug, Clone)]
struct TelegramInstance {
    config: TelegramConfig,
    client: Arc<TelegramBot>,
}

#[cfg(feature = "discord")]
#[derive(Debug)]
struct DiscordInstance {
    config: DiscordConfig,
    client: Arc<DiscordBot>,
}

#[derive(Debug)]
#[cfg(feature = "slack")]
struct SlackInstance {
    config: SlackConfig,
    client: Arc<SlackBot>,
}

pub struct ChatClients {
    #[cfg(feature = "telegram")]
    telegram: Option<Vec<TelegramInstance>>,
    #[cfg(feature = "discord")]
    discord: Option<Vec<DiscordInstance>>,
    #[cfg(feature = "slack")]
    slack: Option<Vec<SlackInstance>>,
}

impl ChatClients {
    pub async fn new(chat_config: ChatConfig) -> Self {
        #[cfg(feature = "telegram")]
        let telegram = chat_config.telegram.map(|config| {
            config
                .into_iter()
                .map(|config| {
                    let client = Arc::new(TelegramBot::new(&config.bot_token));
                    TelegramInstance { config, client }
                })
                .collect()
        });

        #[cfg(feature = "discord")]
        let discord = chat_config.discord.map(|config| {
            config
                .into_iter()
                .map(|config| {
                    let client = Arc::new(DiscordBot::new(&config.bot_token));
                    DiscordInstance { config, client }
                })
                .collect()
        });

        #[cfg(feature = "slack")]
        let slack = chat_config.slack.map(|config| {
            config
                .into_iter()
                .map(|config| {
                    let client = Arc::new(SlackBot::new(config.bot_token.clone()));
                    SlackInstance { config, client }
                })
                .collect()
        });

        Self {
            #[cfg(feature = "telegram")]
            telegram,
            #[cfg(feature = "discord")]
            discord,
            #[cfg(feature = "slack")]
            slack,
        }
    }

    fn find_accepted_block_range(&self, from_block: &U64, to_block: &U64) -> U64 {
        if from_block > to_block {
            panic!("Invalid range: from_block must be less than or equal to to_block");
        }

        match from_block.overflowing_add(to_block - from_block) {
            (result, false) => result,
            (_, true) => U64::MAX,
        }
    }

    pub fn is_in_block_range_to_send(&self, from_block: &U64, to_block: &U64) -> bool {
        // only 10 blocks at a time else rate limits will kick in
        U64::from(10) <= self.find_accepted_block_range(from_block, to_block)
    }

    fn has_any_chat(&self) -> bool {
        #[cfg(feature = "telegram")]
        let has_telegram = self.telegram.is_some();
        #[cfg(not(feature = "telegram"))]
        let has_telegram = false;

        #[cfg(feature = "discord")]
        let has_discord = self.discord.is_some();
        #[cfg(not(feature = "discord"))]
        let has_discord = false;

        #[cfg(feature = "slack")]
        let has_slack = self.slack.is_some();
        #[cfg(not(feature = "slack"))]
        let has_slack = false;

        has_telegram || has_discord || has_slack
    }

    #[cfg(feature = "telegram")]
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
                    filter_event_data_by_conditions(event_data, conditions)
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

    #[cfg(feature = "discord")]
    fn discord_send_message_tasks(
        &self,
        instance: &DiscordInstance,
        event_for: &DiscordEvent,
        events_data: &[Value],
    ) -> SendMessage {
        let tasks: Vec<_> = events_data
            .iter()
            .filter(|event_data| {
                if let Some(conditions) = &event_for.conditions {
                    filter_event_data_by_conditions(event_data, conditions)
                } else {
                    true
                }
            })
            .map(|event_data| {
                let client = Arc::clone(&instance.client);
                let channel_id = ChannelId::new(instance.config.channel_id);
                let message = Template::new(event_for.template_inline.clone())
                    .parse_template_inline(event_data);
                task::spawn(async move {
                    client.send_message(channel_id, &message).await?;
                    Ok(())
                })
            })
            .collect();
        tasks
    }

    #[cfg(feature = "slack")]
    fn slack_send_message_tasks(
        &self,
        instance: &SlackInstance,
        event_for: &SlackEvent,
        events_data: &[Value],
    ) -> SendMessage {
        let tasks: Vec<_> = events_data
            .iter()
            .filter(|event_data| {
                if let Some(conditions) = &event_for.conditions {
                    filter_event_data_by_conditions(event_data, conditions)
                } else {
                    true
                }
            })
            .map(|event_data| {
                let client = Arc::clone(&instance.client);
                let channel = instance.config.channel.clone();
                let message = Template::new(event_for.template_inline.clone())
                    .parse_template_inline(event_data);
                task::spawn(async move {
                    client.send_message(&channel, &message).await?;
                    Ok(())
                })
            })
            .collect();
        tasks
    }

    pub async fn send_message(
        &self,
        event_message: &EventMessage,
        index_event_in_order: bool,
        from_block: &U64,
        to_block: &U64,
    ) -> Result<usize, ChatError> {
        if !self.has_any_chat() || !self.is_in_block_range_to_send(from_block, to_block) {
            return Ok(0);
        }

        // will always have something even if the event has no parameters due to the tx_information
        if let Value::Array(data_array) = &event_message.event_data {
            let mut messages: Vec<SendMessage> = Vec::new();

            #[cfg(feature = "telegram")]
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

            #[cfg(feature = "discord")]
            if let Some(discord) = &self.discord {
                for instance in discord {
                    if instance.config.networks.contains(&event_message.network) {
                        let discord_event = instance
                            .config
                            .messages
                            .iter()
                            .find(|e| e.event_name == event_message.event_name);

                        if let Some(discord_event) = discord_event {
                            let message = self.discord_send_message_tasks(
                                instance,
                                discord_event,
                                data_array,
                            );
                            messages.push(message);
                        }
                    }
                }
            }

            #[cfg(feature = "slack")]
            if let Some(slack) = &self.slack {
                for instance in slack {
                    if instance.config.networks.contains(&event_message.network) {
                        let slack_event = instance
                            .config
                            .messages
                            .iter()
                            .find(|e| e.event_name == event_message.event_name);

                        if let Some(slack_event) = slack_event {
                            let message =
                                self.slack_send_message_tasks(instance, slack_event, data_array);
                            messages.push(message);
                        }
                    }
                }
            }

            let mut messages_sent = 0;

            if index_event_in_order {
                for message in messages {
                    for publish in message {
                        match publish.await {
                            Ok(Ok(_)) => messages_sent += 1,
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(ChatError::JoinError(e)),
                        }
                    }
                }
            } else {
                let tasks: Vec<_> = messages.into_iter().flatten().collect();
                let results = join_all(tasks).await;
                for result in results {
                    match result {
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
