use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage};
use serde_derive::Deserialize;

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

#[tokio::main]
async fn main() {
    let config: Config = envy::from_env().expect("ENV is not valid");

    let pubsub = Client::new(config.google_application_credentials)
        .await
        .expect("Failed to initialize pubsub");

    let topic = pubsub.topic(config.topic);

    let sub = topic.subscribe().await.expect("Failed to subscribe");

    println!("Subscribed to topic with: {}", sub.name);
    let packets = sub
        .clone()
        .get_messages::<UpdatePacket>(100)
        .await
        .expect("Error Checking PubSub");

    for packet in &packets {
        println!("Received: {:?}", packet);
    }

    if !packets.is_empty() {
        let acks = packets
            .into_iter()
            .map(|packet| packet.1)
            .collect::<Vec<_>>();
        sub.acknowledge_messages(acks).await;
    } else {
        println!("Cleaning up");
        drop(pubsub);
        sub.destroy().await.expect("Failed deleting subscription");
        println!("Successfully deleted subscription");
    }
}
