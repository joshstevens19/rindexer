use std::any::Any;
use std::collections::HashMap;

// Structure for the event callback registry
pub struct EventCallbackRegistry {
    events: HashMap<&'static str, Box<dyn Fn(&dyn Any)>>,
}

impl EventCallbackRegistry {
    // Create a new, empty registry
    pub fn new() -> Self {
        EventCallbackRegistry {
            events: HashMap::new(),
        }
    }

    // Register an event with a given topic ID and its callback
    pub fn register_event(&mut self, topic_id: &'static str, callback: Box<dyn Fn(&dyn Any)>) {
        self.events.insert(topic_id, callback);
    }

    // Trigger an event by topic ID, passing arbitrary data to the callback
    pub fn trigger_event(&self, topic_id: &'static str, data: &dyn Any) {
        if let Some(event) = self.events.get(topic_id) {
            event(data);
        }
    }
}
