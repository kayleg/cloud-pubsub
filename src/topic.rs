use crate::client::Client;
use crate::error;
use crate::subscription::*;
use futures::prelude::*;
use hyper::Method;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Topic {
    pub name: String,

    #[serde(skip)]
    pub(crate) client: Option<Client>,
}

impl Topic {
    pub fn subscribe(&self) -> impl Future<Item = Subscription, Error = error::Error> {
        let client = self
            .client
            .clone()
            .expect("Topic must be created using a client");

        let canonical_name = format!(
            "projects/{}/subscriptions/RST{}",
            client.project(),
            thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .collect::<String>()
        );
        let uri: hyper::Uri = format!("https://pubsub.googleapis.com/v1/{}", canonical_name)
            .parse()
            .unwrap();

        let json = serde_json::to_string(&Subscription {
            name: canonical_name,
            topic: Some(self.name.clone()),
            client: None,
        })
        .expect("Failed to serialize subscription");

        let mut req = client.request(Method::PUT, json);
        *req.uri_mut() = uri.clone();

        client
            .hyper_client()
            .request(req)
            .and_then(|res| res.into_body().concat2())
            .from_err::<error::Error>()
            .and_then(move |body| {
                let mut sub = serde_json::from_slice::<Subscription>(&body)?;
                sub.client = Some(client.clone());
                Ok(sub)
            })
            .from_err::<error::Error>()
    }
}
