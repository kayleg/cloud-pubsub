use cloud_pubsub::error;
use cloud_pubsub::{Client, EncodedMessage, FromPubSubMessage, Subscription, Topic};
use futures::future::lazy;
use serde_derive::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::prelude::*;
use ctrlc;
use std::env;

pub struct Worker {
  pub client: Client,
  pub topics: Vec<Topic>,
}

pub struct WorkerWrapper {
  pub worker: Arc<Worker>
}

impl WorkerWrapper {
  pub fn new() -> WorkerWrapper {
    WorkerWrapper {
      worker: Arc::new(Worker::new())
    }
  }

  pub fn run(&self) {
      let topics = Arc::new(self.worker.topics);
      tokio::run(lazy(move || {
        self.worker.client.spawn_token_renew(Duration::from_secs(60));
        for topic in topics.iter() {
          topic.subscribe().map(move |subscription| {
            println!("Subscribed to topic with: {}", subscription.name);
            let atomic_sub = Arc::new(subscription);
            self.worker.schedule_pubsub_pull(&atomic_sub);
            
            ctrlc::set_once_handler(move || {
              println!("Cleaning up");
              self.worker.client.stop();
              self.worker.wait_until_pull_is_finished(&atomic_sub);

              Arc::try_unwrap(atomic_sub).and_then(|s| {
                let destroy = s.destroy()
                            .map(|_r| println!("Successfully deleted subscription"))
                            .map_err(|e| eprintln!("Failed deleting subscription: {}", e));
              tokio::run(destroy);
              Ok(())
              });
            }).expect("Error setting Ctrl-C handler");
        });
      }
      Ok(())
    }));
  }
}

fn main() {
  WorkerWrapper::new().run();
}

impl Worker {
  pub fn new() -> Worker {
    let credentials_file: String = env::var("GOOGLE_APPLICATION_CREDENTIALS")
      .expect("GOOGLE_APPLICATION_CREDENTIALS must be set");
    
    Client::new(credentials_file)
      .and_then(|client| {
        let topics_str: String = env::var("TOPIC_NAMES").unwrap_or(String::from(""));
        let topics: Vec<Topic> = topics_str.split(",").map(|name| client.topic(name.into())).collect();
        Ok(Self { client, topics })
      })
      .unwrap()
  }

  pub fn wait_until_pull_is_finished(&self, sub: &Arc<Subscription>) {
    println!("Waiting for current Pull to finish....");
    while Arc::strong_count(sub) > 1 { }
  }
  
  pub fn schedule_pubsub_pull(&self, subscription: &Arc<Subscription>) {
    let sub = Arc::clone(&subscription);
    let get = subscription
        .clone()
        .get_messages::<Event>()
        .map(move |(events, acks)| {
            for event in events {
                println!("Received: {:?}", event);
            }
            if !acks.is_empty() {
                tokio::spawn(subscription.acknowledge_messages(acks));
            }
        })
        .map_err(|e| println!("Error Checking PubSub: {}", e))
        .and_then(move |_| {
            if sub.client().is_running() {
                self.schedule_pubsub_pull(&sub);
            } else {
                println!("No longer pulling");
            }

            Ok(())
        });
    tokio::spawn(get);
  }
}

#[derive(Debug, Deserialize)]
struct Event {
    pub name: String,
    pub data: String
}

impl FromPubSubMessage for Event {
    fn from(message: EncodedMessage) -> Result<Self, error::Error> {
        match message.decode() {
            Ok(bytes) => {
                serde_json::from_slice::<Event>(&bytes).map_err(error::Error::from)
            },
            Err(e) => Err(error::Error::from(e)),
        }
    }
}
