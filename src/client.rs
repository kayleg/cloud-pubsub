use crate::error;
use crate::subscription::Subscription;
use crate::topic::Topic;
use futures::prelude::*;
use goauth::auth::JwtClaims;
use goauth::scopes::Scope;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use smpl_jwt::Jwt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::timer::Interval;

type HyperClient = Arc<hyper::Client<HttpsConnector<HttpConnector>, hyper::Body>>;

pub struct State {
    token: Option<goauth::auth::Token>,
    credentials_path: String,
    project: Option<String>,
    hyper_client: HyperClient,
}

impl State {
    pub fn token_type(&self) -> &str {
        self.token.as_ref().unwrap().token_type()
    }

    pub fn access_token(&self) -> &str {
        self.token.as_ref().unwrap().access_token()
    }

    pub fn project(&self) -> &str {
        &(self.project.as_ref().expect("Google Cloud Project has not been set. If it is not in your credential file, call set_project to set it manually."))
    }
}

pub struct Client(Arc<RwLock<State>>);

impl Clone for Client {
    fn clone(&self) -> Self {
        Client(self.0.clone())
    }
}

impl Client {
    pub fn new(credentials_path: String) -> Result<Self, error::Error> {
        let mut client = Client(Arc::new(RwLock::new(State {
            token: None,
            credentials_path,
            project: None,
            hyper_client: setup_hyper(),
        })));

        match client.refresh_token() {
            Ok(_) => Ok(client),
            Err(e) => Err(e),
        }
    }

    pub fn subscribe(&self, name: String) -> Subscription {
        Subscription {
            client: Some(self.clone()),
            name: format!("projects/{}/subscriptions/{}", self.project(), name),
            topic: None,
        }
    }

    pub fn set_project(&mut self, project: String) {
        self.0.write().unwrap().project = Some(project);
    }

    pub fn project(&self) -> String {
        self.0.read().unwrap().project().to_string()
    }

    pub fn topic(&self, name: String) -> Topic {
        Topic {
            client: Some(Client(self.0.clone())),
            name: format!("projects/{}/topics/{}", self.project(), name),
        }
    }

    pub fn spawn_token_renew(&self) {
        let mut client = self.clone();
        let renew_token_task = Interval::new(Instant::now(), Duration::from_secs(15 * 60))
            .for_each(move |_instant| {
                println!("Renewing pubsub token");
                if let Err(e) = client.refresh_token() {
                    eprintln!("Failed to update token: {}", e);
                }
                Ok(())
            })
            .map_err(|e| eprintln!("token refresh interval errored; err={:?}", e));

        tokio::spawn(renew_token_task);
    }

    pub fn refresh_token(&mut self) -> Result<(), error::Error> {
        match self.get_token() {
            Ok(token) => {
                self.0.write().unwrap().token = Some(token);
                Ok(())
            }
            Err(e) => Err(error::Error::from(e)),
        }
    }

    fn get_token(&mut self) -> Result<goauth::auth::Token, goauth::error::GOErr> {
        let credentials =
            goauth::credentials::Credentials::from_file(&self.0.read().unwrap().credentials_path)
                .unwrap();

        self.set_project(credentials.project());

        let claims = JwtClaims::new(
            credentials.iss(),
            &Scope::PubSub,
            credentials.token_uri(),
            None,
            None,
        );
        let jwt = Jwt::new(claims, credentials.rsa_key().unwrap(), None);
        goauth::get_token_with_creds(&jwt, &credentials)
    }

    pub(crate) fn request<T: Into<hyper::Body>>(
        &self,
        method: hyper::Method,
        data: T,
    ) -> hyper::Request<hyper::Body>
    where
        hyper::Body: std::convert::From<T>,
    {
        let mut req = hyper::Request::new(hyper::Body::from(data));
        *req.method_mut() = method;
        req.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/json"),
        );
        let readable = self.0.read().unwrap();
        req.headers_mut().insert(
            hyper::header::AUTHORIZATION,
            hyper::header::HeaderValue::from_str(&format!(
                "{} {}",
                readable.token_type(),
                readable.access_token()
            ))
            .unwrap(),
        );
        req
    }

    pub fn hyper_client(&self) -> HyperClient {
        self.0.read().unwrap().hyper_client.clone()
    }
}

fn setup_hyper() -> HyperClient {
    // 4 is number of blocking DNS threads
    let https = HttpsConnector::new(4).unwrap();
    Arc::new(hyper::Client::builder().build::<_, hyper::Body>(https))
}
