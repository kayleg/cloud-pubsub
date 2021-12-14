use cloud_pubsub::Client;
use serde_derive::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct Config {
    topic: String,
    google_application_credentials: String,
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
        Ok(p) => Arc::new(p),
    };

    let topic = Arc::new(pubsub.topic(config.topic.clone()).await);
    match topic.clone().publish("🔥").await {
        Ok(response) => {
            println!("{:?}", response);
            pubsub.stop().await;
            std::process::exit(0);
        }
        Err(e) => eprintln!("Failed sending message {}", e),
    }
}
