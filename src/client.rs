use crate::error;
use crate::subscription::Subscription;
use futures::prelude::*;
use goauth::auth::JwtClaims;
use goauth::scopes::Scope;
use smpl_jwt::Jwt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::timer::Interval;

pub struct State {
    token: Option<goauth::auth::Token>,
    credentials_path: String,
}

impl State {
    pub fn token_type(&self) -> &str {
        self.token.as_ref().unwrap().token_type()
    }

    pub fn access_token(&self) -> &str {
        self.token.as_ref().unwrap().access_token()
    }
}

pub trait Client
where
    Self: std::marker::Sized + TokenAuthenticated,
{
    fn create(credentials_path: String) -> Result<Self, error::Error>;
    fn subscribe(&self, name: String) -> Subscription;
}

pub trait TokenAuthenticated {
    fn spawn_token_renew(&self);
    fn refresh_token(&mut self) -> Result<(), error::Error>;
    fn get_token(&self) -> Result<goauth::auth::Token, goauth::error::GOErr>;
}

pub type BaseClient = Arc<RwLock<State>>;

impl Client for BaseClient {
    fn subscribe(&self, name: String) -> Subscription {
        Subscription {
            client: self.clone(),
            name,
        }
    }

    fn create(credentials_path: String) -> Result<Self, error::Error> {
        let mut client = Arc::new(RwLock::new(State {
            token: None,
            credentials_path,
        }));

        match client.refresh_token() {
            Ok(_) => Ok(client),
            Err(e) => Err(e),
        }
    }
}

impl TokenAuthenticated for BaseClient {
    fn spawn_token_renew(&self) {
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

    fn refresh_token(&mut self) -> Result<(), error::Error> {
        match self.get_token() {
            Ok(token) => {
                self.write().unwrap().token = Some(token);
                Ok(())
            }
            Err(e) => Err(error::Error::from(e)),
        }
    }

    fn get_token(&self) -> Result<goauth::auth::Token, goauth::error::GOErr> {
        let credentials =
            goauth::credentials::Credentials::from_file(&self.read().unwrap().credentials_path)
                .unwrap();

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
}
