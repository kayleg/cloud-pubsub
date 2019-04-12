use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage};
use futures::future::lazy;
use serde_derive::Deserialize;
use std::sync::Arc;
use tokio::prelude::*;

#[derive(Deserialize)]
struct Config {
    topic: String,
    google_application_credentials: String,
}

#[derive(Debug)]
struct UpdatePacket(String);

impl FromPubSubMessage for UpdatePacket {
    fn from(message: EncodedMessage) -> Result<Self, error::Error> {
        match message.decode() {
            Ok(bytes) => Ok(UpdatePacket(String::from_utf8_lossy(&bytes).into_owned())),
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

    let pubsub = match Client::new(config.google_application_credentials) {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => p,
    };

    let topic = Arc::new(pubsub.topic(config.topic));

    tokio::run(lazy(move || {
        topic
            .subscribe()
            .map(|subscription| {
                println!("Subscribed to topic with: {}", subscription.name);
                let sub = Arc::new(subscription);
                let get = sub
                    .clone()
                    .get_messages::<UpdatePacket>()
                    .map(move |(packets, acks)| {
                        for packet in packets {
                            println!("Received: {:?}", packet);
                        }

                        if !acks.is_empty() {
                            tokio::spawn(sub.acknowledge_messages(acks));
                        } else {
                            println!("Cleaning up");
                            if let Ok(s) = Arc::try_unwrap(sub) {
                                let destroy = s
                                    .destroy()
                                    .map(|_r| println!("Successfully deleted subscription"))
                                    .map_err(|e| eprintln!("Failed deleting subscription: {}", e))
                                    .then(move |_| {
                                        drop(pubsub);
                                        Ok(())
                                    });
                                tokio::spawn(destroy);
                            } else {
                                eprintln!("Subscription is still owned");
                            }
                        }
                    })
                    .map_err(|e| println!("Error Checking PubSub: {}", e));
                tokio::spawn(get);
            })
            .map_err(|e| eprintln!("Failed to subscribe: {}", e))
    }));
}
