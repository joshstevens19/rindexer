use std::any::Any;

use ethers::providers::{Http, Provider};

use crate::manifest::yaml::Source;

pub struct EventInformation {
    pub topic_id: &'static str,
    pub source: Source,
    pub provider: &'static Provider<Http>,
    pub callback: Box<dyn Fn(&dyn Any)>,
}

pub struct EventCallbackRegistry {
    pub events: Vec<EventInformation>,
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new() }
    }

    pub fn find_event(&self, topic_id: &'static str) -> Option<&EventInformation> {
        self.events.iter().find(|e| e.topic_id == topic_id)
    }

    pub fn register_event(&mut self, event: EventInformation) {
        self.events.push(event);
    }

    pub fn trigger_event(&self, topic_id: &'static str, data: &dyn Any) {
        if let Some(callback) = self.find_event(&topic_id).map(|e| &e.callback) {
            callback(data);
        } else {
            println!(
                "EventCallbackRegistry: No event found for topic_id: {}",
                topic_id
            );
        }
    }
}
