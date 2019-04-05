use crate::error;
use base64;
use serde_derive::Deserialize;

#[derive(Deserialize)]
pub struct EncodedMessage {
    data: String,
}

pub trait FromPubSubMessage
where
    Self: std::marker::Sized,
{
    fn from(message: EncodedMessage) -> Result<Self, error::Error>;
}

impl EncodedMessage {
    pub fn decode(&self) -> Result<Vec<u8>, base64::DecodeError> {
        base64::decode(&self.data)
    }
}

#[derive(Deserialize)]
pub(crate) struct Message {
    #[serde(alias = "ackId")]
    pub(crate) ack_id: String,
    pub(crate) message: EncodedMessage,
}
