use cloud_pubsub::error;
use cloud_pubsub::{BaseClient, Client, EncodedMessage, FromPubSubMessage};
use futures::future::lazy;
use serde_derive::Deserialize;
use std::sync::Arc;
use tokio::prelude::*;

#[derive(Deserialize)]
struct Config {
    pubsub_subscription: String,
    google_application_credentials: String,
}

#[derive(Debug, Deserialize)]
struct UpdatePacket {
    id: u64,
    name: String,
}

impl FromPubSubMessage for UpdatePacket {
    fn from(message: EncodedMessage) -> Result<Self, error::Error> {
        match message.decode() {
            Ok(bytes) => serde_json::from_slice::<UpdatePacket>(&bytes).map_err(error::Error::from),
            Err(e) => Err(error::Error::from(e)),
        }
    }
}

fn main() {
    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        eprintln!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match BaseClient::create(config.google_application_credentials) {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => p,
    };

    let order_sub = Arc::new(pubsub.subscribe(config.pubsub_subscription));

    tokio::run(lazy(move || {
        order_sub
            .clone()
            .get_messages::<UpdatePacket>()
            .map(move |(packets, acks)| {
                for packet in packets {
                    println!("Received: {:?}", packet);
                }

                if !acks.is_empty() {
                    tokio::spawn(order_sub.acknowledge_messages(acks));
                }
            })
            .map_err(|e| println!("Error Checking PubSub: {}", e))
    }));
}
