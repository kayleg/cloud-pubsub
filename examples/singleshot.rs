use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage};
use serde_derive::Deserialize;
use std::sync::Arc;
use tokio::task;

#[derive(Deserialize)]
struct Config {
    pubsub_subscription: String,
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

#[tokio::main]
async fn main() {
    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        eprintln!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match Client::new(config.google_application_credentials).await {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(p) => p,
    };

    let subscription = Arc::new(pubsub.subscribe(config.pubsub_subscription));
    match subscription.get_messages::<UpdatePacket>(100).await {
        Ok(messages) => {
            for (result, ack_id) in messages {
                match result {
                    Ok(message) => {
                        println!("Received: {:?}", message);
                        let subscription = Arc::clone(&subscription);
                        task::spawn(async move {
                            subscription.acknowledge_messages(vec![ack_id]).await;
                        });
                    }
                    Err(e) => log::error!("Failed converting to UpdatePacket: {}", e),
                }
            }
        }
        Err(e) => eprintln!("Failed to pull PubSub messages: {}", e),
    }
}
