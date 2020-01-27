use serde_derive::Deserialize;
use std::fmt;
use std::io;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Error {
    #[serde(skip_deserializing)]
    PubSubAuth(goauth::error::GOErr),
    #[serde(skip_deserializing)]
    Http(hyper::Error),
    #[serde(skip_deserializing)]
    Json(serde_json::Error),
    #[serde(skip_deserializing)]
    Base64(base64::DecodeError),
    #[serde(skip_deserializing)]
    IO(io::Error),
    PubSub {
        code: i32,
        message: String,
        status: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::PubSubAuth(e) => write!(f, "PubSubAuth({})", e),
            Error::Http(e) => write!(f, "Hyper({})", e),
            Error::Json(e) => write!(f, "Json({})", e),
            Error::Base64(e) => write!(f, "Base64({})", e),
            Error::IO(e) => write!(f, "IO({})", e),
            Error::PubSub {
                code,
                message,
                status,
            } => write!(
                f,
                "PubSub(code: {}, status: {}, message: {})",
                code, status, message
            ),
        }
    }
}

impl std::error::Error for Error {}

impl From<goauth::error::GOErr> for Error {
    fn from(err: goauth::error::GOErr) -> Error {
        Error::PubSubAuth(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Error {
        Error::Http(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Error {
        Error::Json(err)
    }
}

impl From<base64::DecodeError> for Error {
    fn from(err: base64::DecodeError) -> Error {
        Error::Base64(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IO(err)
    }
}
