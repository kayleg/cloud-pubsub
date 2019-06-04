use cloud_pubsub::Client;
use futures::future::lazy;
use serde_derive::Deserialize;
use std::sync::Arc;
use tokio::prelude::*;

#[derive(Deserialize)]
struct Config {
    topic: String,
    google_application_credentials: String,
}

fn main() {
    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        eprintln!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match Client::new(config.google_application_credentials) {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => Arc::new(p),
    };

    let topic = Arc::new(pubsub.topic(config.topic.clone()));
    tokio::run(lazy(move || {
        topic
            .clone()
            .publish("🔥")
            .map(move |response| {
                println!("{:?}", response);
                pubsub.stop();
                std::process::exit(0);
            })
            .map_err(|e| eprintln!("Failed to send_message: {}", e))
    }));
}
