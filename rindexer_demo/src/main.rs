mod rindexer;

use std::sync::Arc;

use rindexer::lens_registry_example::{
    contexts::get_injected,
    events::lens_registry::{HandleLinkedEvent, HandleUnlinkedEvent, LensRegistryEventType},
};

use crate::rindexer::lens_registry_example::events::lens_registry::HandleLinkedData;
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start_indexing,
    rindexer_main, AsyncCsvAppender, PostgresClient,
};

#[macro_export]
macro_rules! create_and_register_event {
    ($registry:expr, $event_enum:ident::$event_variant:ident, $data_type:ty, $callback:block) => {{
        use futures::FutureExt;
        use std::sync::Arc; // Make sure futures is in your dependencies for .boxed()

        let event = $event_enum::$event_variant({
            let callback: Arc<dyn Fn(Arc<$data_type>) -> _ + Send + Sync> =
                Arc::new(move |data: Arc<$data_type>| {
                    Box::pin(async move {
                        let data_ref = &*data; // Dereference Arc to get to the data
                        $callback
                    })
                });

            HandleLinkedEvent {
                // Assume HandleLinkedEvent, you can parameterize this as needed
                callback,
            }
        });

        event.register($registry);
    }};
}

#[tokio::main]
async fn main() {
    // generate();

    let mut registry = EventCallbackRegistry::new();

    // LensRegistryEventType::NonceUpdated(NonceUpdatedEvent {
    //     callback: Arc::new(|data| {
    //         println!("NonceUpdated event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleLinked(HandleLinkedEvent {
    //     callback: Arc::new(|data| {
    //         println!("HandleLinked event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleLinked(HandleLinkedEvent {
    //     callback: Arc::new(|data| {
    //         let fut = async move {
    //             // Your asynchronous callback implementation
    //             println!("HandleLinked event: {:?}", data);
    //         };
    //         Box::pin(fut)
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleUnlinked(HandleUnlinkedEvent {
    //     callback: Arc::new(|data| {
    //         println!("HandleUnlinked event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // Create your HandleLinkedEvent instance with its callback function using the macro
    // let handle_linked_event = event_handler!(handle_linked_event<HandleLinkedData>, {
    //     // Your asynchronous callback implementation
    //     // Simulated async operation (e.g., database insertion)
    //     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    //     println!("HandleLinked event: {:?}", data);
    // }, &mut registry);

    // let appender = Arc::new(AsyncCsvAppender::new("events.csv".to_string()));

    LensRegistryEventType::HandleLinked(HandleLinkedEvent {
        callback: Arc::new(|data| {
            println!("HandleLinked event: {:?}", data);
            // let appender = Arc::new(AsyncCsvAppender::new("events.csv".to_string()));
            Box::pin(async move {
                // Your asynchronous callback implementation
                // Simulated async operation (e.g., database insertion)
                // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                // appender.append(data.clone()).await.unwrap();
                // println!("HandleLinked event: {:?}", data);
            })
        }),
    })
    .register(&mut registry);

    let _ = start_indexing(registry, 100).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        generate();
    }
}

fn generate() {
    build(
        "/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml",
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}
