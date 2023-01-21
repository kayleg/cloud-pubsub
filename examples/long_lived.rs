use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription};
use serde_derive::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::{signal, task};

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
    task::spawn(async move {
        while subscription.client().is_running() {
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
        println!("No longer pulling");
    });
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let parsed_env = envy::from_env::<Config>();
    if let Err(e) = parsed_env {
        eprintln!("ENV is not valid: {}", e);
        std::process::exit(1);
    }
    let config = parsed_env.unwrap();

    let pubsub = match Client::new(config.google_application_credentials).await {
        Err(e) => panic!("Failed to initialize pubsub: {}", e),
        Ok(mut client) => {
            if let Err(e) = client.refresh_token().await {
                eprintln!("Failed to get token: {}", e);
            } else {
                println!("Got fresh token");
            }
            Arc::new(client)
        }
    };

    pubsub.spawn_token_renew(Duration::from_secs(15 * 60));

    let topic = Arc::new(pubsub.topic(config.topic));
    let subscription = topic.subscribe().await?;
    println!("Subscribed to topic with: {}", subscription.name);
    let sub = Arc::new(subscription);
    schedule_pubsub_pull(Arc::clone(&sub));
    signal::ctrl_c().await?;
    println!("Cleaning up");
    pubsub.stop();
    println!("Waiting for current Pull to finish....");
    while Arc::strong_count(&sub) > 1 {}
    println!("Deleting subscription");
    if let Ok(s) = Arc::try_unwrap(sub) {
        s.destroy().await?;
        println!("Successfully deleted subscription");
    } else {
        eprintln!("Subscription was still ownded");
    }
    Ok(())
}
