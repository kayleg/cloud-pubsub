use std::fmt;

pub enum Error {
    PubSubAuth(goauth::error::GOErr),
    Http(hyper::Error),
    Json(serde_json::Error),
    Base64(base64::DecodeError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::PubSubAuth(e) => write!(f, "PubSubAuth({})", e),
            Error::Http(e) => write!(f, "Hyper({})", e),
            Error::Json(e) => write!(f, "Json({})", e),
            Error::Base64(e) => write!(f, "Base64({})", e),
        }
    }
}

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
