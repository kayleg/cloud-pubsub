use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription};
use ctrlc;
use futures::future::lazy;
use serde_derive::Deserialize;
use std::sync::Arc;
use std::time::Duration;
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

fn schedule_pubsub_pull(subscription: Arc<Subscription>) {
    let sub = Arc::clone(&subscription);
    let get = subscription
        .clone()
        .get_messages::<UpdatePacket>()
        .map(move |(packets, acks)| {
            for packet in packets {
                println!("Received: {:?}", packet);
            }

            if !acks.is_empty() {
                tokio::spawn(subscription.acknowledge_messages(acks));
            }
        })
        .map_err(|e| println!("Error Checking PubSub: {}", e))
        .and_then(|_| {
            if sub.client().is_running() {
                schedule_pubsub_pull(sub);
            } else {
                println!("No longer pulling");
            }

            Ok(())
        });
    tokio::spawn(get);
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

    let topic = Arc::new(pubsub.topic(config.topic));

    tokio::run(lazy(move || {
        pubsub.spawn_token_renew(Duration::from_secs(2));

        topic
            .subscribe()
            .map(move |subscription| {
                println!("Subscribed to topic with: {}", subscription.name);
                let sub = Arc::new(subscription);

                schedule_pubsub_pull(Arc::clone(&sub));

                ctrlc::set_once_handler(move || {
                    println!("Cleaning up");
                    pubsub.stop();
                    let client = Arc::clone(&pubsub);
                    println!("Waiting for current Pull to finish....");
                    while Arc::strong_count(&sub) > 1 {}
                    println!("Deleting subscription");
                    if let Ok(s) = Arc::try_unwrap(sub) {
                        let destroy = s
                            .destroy()
                            .map(|_r| println!("Successfully deleted subscription"))
                            .map_err(|e| eprintln!("Failed deleting subscription: {}", e))
                            .then(move |_| {
                                if let Ok(c) = Arc::try_unwrap(client) {
                                    drop(c);
                                }
                                Ok(())
                            });
                        tokio::run(destroy);
                    } else {
                        eprintln!("Subscription was still ownded");
                    }
                })
                .expect("Error setting Ctrl-C handler");
            })
            .map_err(|e| eprintln!("Failed to subscribe: {}", e))
    }));
}
