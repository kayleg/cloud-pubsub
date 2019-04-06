use crate::client::State;
use crate::error;
use crate::message::{FromPubSubMessage, Message};
use futures::prelude::*;
use hyper::{header::HeaderValue, Body, Method, Request};
use hyper_tls::HttpsConnector;
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

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

pub struct Subscription {
    pub(crate) client: Arc<RwLock<State>>,
    pub name: String,
    pub(crate) canonical_name: String,
}

impl Subscription {
    pub fn acknowledge_messages(&self, ids: Vec<String>) -> impl Future<Item = (), Error = ()> {
        let https = HttpsConnector::new(4).unwrap();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);

        let uri: hyper::Uri = format!(
            "https://pubsub.googleapis.com/v1/{}:acknowledge",
            self.canonical_name
        )
        .parse()
        .unwrap();

        let json = serde_json::to_string(&AckRequest { ack_ids: ids }).unwrap();

        let mut req = Request::new(Body::from(json));
        *req.method_mut() = Method::POST;
        *req.uri_mut() = uri.clone();
        req.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let readable = self.client.read().unwrap();
        req.headers_mut().insert(
            hyper::header::AUTHORIZATION,
            HeaderValue::from_str(&format!(
                "{} {}",
                readable.token_type(),
                readable.access_token()
            ))
            .unwrap(),
        );

        client
            .request(req)
            .and_then(|_response| Ok(()))
            .map_err(|e| eprintln!("Failed ACk: {}", e))
    }

    pub fn get_messages<T: FromPubSubMessage>(
        &self,
    ) -> impl Future<Item = (Vec<T>, Vec<String>), Error = error::Error> {
        // 4 is number of blocking DNS threads
        let https = HttpsConnector::new(4).unwrap();
        let client = hyper::Client::builder().build::<_, hyper::Body>(https);

        let uri: hyper::Uri = format!(
            "https://pubsub.googleapis.com/v1/{}:pull",
            self.canonical_name
        )
        .parse()
        .unwrap();

        let json = r#"{"maxMessages": 100}"#;

        let mut req = Request::new(Body::from(json));
        *req.method_mut() = Method::POST;
        *req.uri_mut() = uri.clone();
        req.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let readable = self.client.read().unwrap();
        req.headers_mut().insert(
            hyper::header::AUTHORIZATION,
            HeaderValue::from_str(&format!(
                "{} {}",
                readable.token_type(),
                readable.access_token()
            ))
            .unwrap(),
        );

        client
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
}
