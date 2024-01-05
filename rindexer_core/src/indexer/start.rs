use crate::generator::event_callback_registry::EventCallbackRegistry;

pub fn start(registry: EventCallbackRegistry) -> () {
    registry.trigger_event("0xc906270cebe7667882104effe64262a73c422ab9176a111e05ea837b021065fc", &(1));
}
