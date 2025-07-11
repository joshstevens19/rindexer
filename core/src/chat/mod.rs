#[cfg(any(feature = "discord", feature = "slack", feature = "telegram"))]
mod clients;
#[cfg(any(feature = "discord", feature = "slack", feature = "telegram"))]
pub use clients::ChatClients;

#[cfg(feature = "discord")]
mod discord;
#[cfg(feature = "slack")]
mod slack;
#[cfg(feature = "telegram")]
mod telegram;
#[cfg(feature = "telegram")]
mod template;
