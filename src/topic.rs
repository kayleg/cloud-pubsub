use crate::client::Client;
use crate::error;
use crate::subscription::*;
use crate::EncodedMessage;
use bytes::buf::BufExt as _;
use hyper::Method;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};

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

        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}", new_subscription.name)
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
        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}:publish", self.name)
            .parse()
            .unwrap();

        let new_message = EncodedMessage::new(&data);
        let payload = PublishMessageRequest {
            messages: vec![new_message],
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
        let body = hyper::body::aggregate(response).await?;
        serde_json::from_reader(body.reader())?
    }

    fn new_subscription_name(&self) -> String {
        let project = self.client.clone().unwrap().project();
        let slug = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .collect::<String>();

        format!("projects/{}/subscriptions/RST{}", project, slug)
    }
}
