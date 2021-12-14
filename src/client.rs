use crate::error;
use crate::subscription::Subscription;
use crate::topic::Topic;
use goauth::auth::JwtClaims;
use goauth::scopes::Scope;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use smpl_jwt::Jwt;
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task;
use tokio::time;

type HyperClient = Arc<hyper::Client<HttpsConnector<HttpConnector>, hyper::Body>>;

pub struct State {
    token: Option<goauth::auth::Token>,
    credentials_string: String,
    project: Option<String>,
    hyper_client: HyperClient,
    running: Arc<AtomicBool>,
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
    pub async fn from_string(credentials_string: String) -> Result<Self, error::Error> {
        let mut client = Client(Arc::new(RwLock::new(State {
            token: None,
            credentials_string,
            project: None,
            hyper_client: setup_hyper(),
            running: Arc::new(AtomicBool::new(true)),
        })));

        match client.refresh_token().await {
            Ok(_) => Ok(client),
            Err(e) => Err(e),
        }
    }

    pub async fn new(credentials_path: String) -> Result<Self, error::Error> {
        let credentials_string = fs::read_to_string(credentials_path).unwrap();
        Self::from_string(credentials_string).await
    }

    pub async fn subscribe(&self, name: String) -> Subscription {
        Subscription {
            client: Some(self.clone()),
            name: format!("projects/{}/subscriptions/{}", self.project().await, name),
            topic: None,
        }
    }

    pub async fn set_project(&mut self, project: String) {
        self.0.write().await.project = Some(project);
    }

    pub async fn project(&self) -> String {
        self.0.read().await.project().to_string()
    }

    pub async fn topic(&self, name: String) -> Topic {
        Topic {
            client: Some(Client(self.0.clone())),
            name: format!("projects/{}/topics/{}", self.project().await, name),
        }
    }

    pub async fn is_running(&self) -> bool {
        self.0.read().await.running.load(Ordering::SeqCst)
    }

    pub async fn stop(&self) {
        self.0.write().await.running.store(false, Ordering::SeqCst)
    }

    pub fn spawn_token_renew(&self, interval: Duration) {
        let mut client = self.clone();
        let c = self.clone();
        let renew_token_task = async move {
            let mut int = time::interval(interval);
            loop {
                if c.is_running().await {
                    int.tick().await;
                    log::debug!("Renewing pubsub token");
                    if let Err(e) = client.refresh_token().await {
                        log::error!("Failed to update token: {}", e);
                    }
                }
            }
        };

        task::spawn(renew_token_task);
    }

    pub async fn refresh_token(&mut self) -> Result<(), error::Error> {
        match self.get_token().await {
            Ok(token) => {
                self.0.write().await.token = Some(token);
                Ok(())
            }
            Err(e) => Err(error::Error::from(e)),
        }
    }

    async fn get_token(&mut self) -> Result<goauth::auth::Token, goauth::GoErr> {
        let credentials =
            goauth::credentials::Credentials::from_str(&self.0.read().await.credentials_string)
                .unwrap();

        self.set_project(credentials.project()).await;

        let claims = JwtClaims::new(
            credentials.iss(),
            &Scope::PubSub,
            credentials.token_uri(),
            None,
            None,
        );
        let jwt = Jwt::new(claims, credentials.rsa_key().unwrap(), None);
        goauth::get_token(&jwt, &credentials).await
    }

    pub(crate) async fn request<T: Into<hyper::Body>>(
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
        let readable = self.0.read().await;
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

    pub async fn hyper_client(&self) -> HyperClient {
        self.0.read().await.hyper_client.clone()
    }
}

fn setup_hyper() -> HyperClient {
    let https = HttpsConnector::new();
    Arc::new(hyper::Client::builder().build::<_, hyper::Body>(https))
}
