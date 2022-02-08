use crate::client::Client;
use crate::error;
use crate::subscription::*;
use crate::EncodedMessage;
use hyper::body::Buf;
use hyper::{Method, StatusCode};
use lazy_static::lazy_static;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use std::env;

lazy_static! {
    static ref PUBSUB_HOST: String = env::var("PUBSUB_EMULATOR_HOST")
        .map(|host| format!("http://{}", host))
        .unwrap_or_else(|_| String::from("https://pubsub.googleapis.com"));
}

#[derive(Deserialize, Serialize)]
pub struct Topic {
    pub name: String,

    #[serde(skip)]
    pub(crate) client: Option<Client>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PublishMessageResponse {
    pub message_ids: Vec<String>,
}

#[derive(Serialize, Clone)]
pub struct PublishMessageRequest {
    pub messages: Vec<EncodedMessage>,
}

impl Topic {
    pub async fn subscribe(&self) -> Result<Subscription, error::Error> {
        let client = self.client.clone();

        let new_subscription = Subscription {
            name: self.new_subscription_name(),
            topic: Some(self.name.clone()),
            client: None,
        };

        let uri: hyper::Uri = format!("{}/v1/{}", *PUBSUB_HOST, new_subscription.name)
            .parse()
            .unwrap();

        let mut sub = self
            .perform_request::<Subscription, Subscription>(uri, Method::PUT, new_subscription)
            .await?;

        sub.client = client.clone();
        Ok(sub)
    }

    pub async fn publish<T: serde::Serialize>(
        &self,
        data: T,
    ) -> Result<PublishMessageResponse, error::Error> {
        self.publish_message(EncodedMessage::new(&data, None)).await
    }

    pub async fn publish_message(
        &self,
        message: EncodedMessage,
    ) -> Result<PublishMessageResponse, error::Error> {
        let uri: hyper::Uri = format!("{}/v1/{}:publish", *PUBSUB_HOST, self.name)
            .parse()
            .unwrap();

        let payload = PublishMessageRequest {
            messages: vec![message],
        };

        self.perform_request::<PublishMessageRequest, PublishMessageResponse>(
            uri,
            Method::POST,
            payload,
        )
        .await
    }

    async fn perform_request<T: serde::Serialize, U: DeserializeOwned + Clone>(
        &self,
        uri: hyper::Uri,
        method: Method,
        data: T,
    ) -> Result<U, error::Error> {
        let client = self
            .client
            .clone()
            .expect("Topic must be created using a client");

        let json = serde_json::to_string(&data).expect("Failed to serialize request body.");
        let mut req = client.request(method, json);
        *req.uri_mut() = uri;

        let response = client.hyper_client().request(req).await?;
        match response.status() {
            StatusCode::NOT_FOUND => Err(error::Error::PubSub {
                code: 404,
                status: "Topic Not Found".to_string(),
                message: self.name.clone(),
            }),
            StatusCode::OK => {
                let body = hyper::body::aggregate(response).await?;
                serde_json::from_reader(body.reader()).map_err(|e| e.into())
            }
            code => {
                let body = hyper::body::aggregate(response).await?;
                let mut buf = String::new();
                use std::io::Read;
                body.reader().read_to_string(&mut buf)?;
                Err(error::Error::PubSub {
                    code: code.as_u16() as i32,
                    status: "Error occurred attempting to subscribe".to_string(),
                    message: buf,
                })
            }
        }
    }

    fn new_subscription_name(&self) -> String {
        let project = self.client.clone().unwrap().project();
        let slug = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();

        format!("projects/{}/subscriptions/RST{}", project, slug)
    }
}
