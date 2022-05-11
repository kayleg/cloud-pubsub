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

    let order_sub = Arc::new(pubsub.subscribe(config.pubsub_subscription));
    match order_sub.clone().get_messages::<UpdatePacket>(100).await {
        Ok(packets) => {
            for packet in &packets {
                println!("Received: {:?}", packet);
            }

            let acks: Vec<String> = packets
                .into_iter()
                .map(|packet| packet.1)
                .collect::<Vec<_>>();
            if !acks.is_empty() {
                task::spawn(async move { order_sub.acknowledge_messages(acks).await })
                    .await // This will block until acknowledgement is complete
                    .expect("Failed to acknoweldge messages");
            }
        }
        Err(e) => println!("Error Checking PubSub: {}", e),
    }
}
