use crate::client::Client;
use crate::error;
use crate::message::{FromPubSubMessage, Message};
use futures::prelude::*;
use hyper::Method;
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Response {
    #[serde(alias = "receivedMessages")]
    received_messages: Option<Vec<Message>>,
    error: Option<error::Error>,
}

#[derive(Serialize)]
struct AckRequest {
    #[serde(alias = "ackIds")]
    ack_ids: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct Subscription {
    #[serde(skip_serializing)]
    pub name: String,
    pub topic: Option<String>,

    #[serde(skip)]
    pub(crate) client: Option<Client>,
}

impl Subscription {
    pub fn acknowledge_messages(&self, ids: Vec<String>) -> impl Future<Item = (), Error = ()> {
        let client = self
            .client
            .as_ref()
            .expect("Subscription was not created using a client");

        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}:acknowledge", self.name)
            .parse()
            .unwrap();

        let json = serde_json::to_string(&AckRequest { ack_ids: ids }).unwrap();

        let mut req = client.request(Method::POST, json);
        *req.uri_mut() = uri.clone();

        client
            .hyper_client()
            .request(req)
            .and_then(|_response| Ok(()))
            .map_err(|e| eprintln!("Failed ACk: {}", e))
    }

    pub fn get_messages<T: FromPubSubMessage>(
        &self,
    ) -> impl Future<Item = (Vec<T>, Vec<String>), Error = error::Error> {
        let client = self
            .client
            .as_ref()
            .expect("Subscription was not created using a client");

        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}:pull", self.name)
            .parse()
            .unwrap();

        let json = r#"{"maxMessages": 100}"#;

        let mut req = client.request(Method::POST, json);
        *req.uri_mut() = uri.clone();

        client
            .hyper_client()
            .request(req)
            .and_then(|res| res.into_body().concat2())
            .from_err::<error::Error>()
            .and_then(|body| {
                let response: Response = serde_json::from_slice(&body)?;
                if let Some(e) = response.error {
                    return Err(e);
                }
                let messages = response.received_messages.unwrap_or_default();
                let ack_ids: Vec<String> = messages
                    .as_slice()
                    .iter()
                    .map(|packet| packet.ack_id.clone())
                    .collect();
                let packets = messages
                    .into_iter()
                    .filter_map(|packet| match T::from(packet.message) {
                        Ok(o) => Some(o),
                        Err(e) => {
                            eprintln!("Failed converting pubsub {}", e,);
                            None
                        }
                    })
                    .collect();

                Ok((packets, ack_ids))
            })
            .from_err()
    }

    pub fn destroy(self) -> impl Future<Item = (), Error = error::Error> {
        let client = self
            .client
            .expect("Subscription was not created using a client");

        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}", self.name)
            .parse()
            .unwrap();

        let mut req = client.request(Method::DELETE, "");
        *req.uri_mut() = uri.clone();

        client
            .hyper_client()
            .request(req)
            .and_then(|_res| Ok(()))
            .from_err::<error::Error>()
    }

    pub fn client(&self) -> &Client {
        self.client.as_ref().unwrap()
    }
}
