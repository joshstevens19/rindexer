use std::any::Any;
use std::collections::HashMap;


pub struct EventCallbackRegistry {
    pub events: HashMap<&'static str, Box<dyn Fn(&dyn Any)>>,
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry {
            events: HashMap::new(),
        }
    }

    pub fn register_event(&mut self, topic_id: &'static str, callback: Box<dyn Fn(&dyn Any)>) {
        self.events.insert(topic_id, callback);
    }

    pub fn trigger_event(&self, topic_id: &'static str, data: &dyn Any) {
        if let Some(event) = self.events.get(topic_id) {
            event(data);
        }
    }
}
