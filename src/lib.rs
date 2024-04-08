use std::fmt::{Display, Formatter};

pub use http;
use http::header::InvalidHeaderValue;
use http::{header, HeaderName, HeaderValue, Method, Request, Uri};
use thiserror::Error;
use url::Url;

pub use r#async::*;

pub use crate::influx::DataPoint;
pub use crate::influx::DataPointBuilder;
use crate::influx::LineProtocol;

mod r#async;
pub mod blocking;
mod influx;

pub const API_ENDPOINT_V2: &str = "/api/v2/write";

pub struct InfluxWriter<W> {
    client: W,
    url: Url,
    authorization: Authorization,
    org: String,
    bucket: String,
}

impl<W> InfluxWriter<W> {
    pub(crate) fn build_request(
        &self,
        point: impl IntoIterator<Item = DataPoint>,
        precision: WritePrecision,
    ) -> anyhow::Result<Request<String>> {
        let mut url = self.url.clone();
        url.query_pairs_mut().extend_pairs([
            ("org", &self.org),
            ("bucket", &self.bucket),
            ("precision", &precision.to_string()),
        ]);

        Ok(http::request::Builder::new()
            .uri(Uri::try_from(url.as_str())?)
            .header(header::USER_AGENT, "influx-write/0.0.0")
            .header(header::AUTHORIZATION, self.authorization.header_value())
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(header::ACCEPT, "application/json")
            .method(Method::POST)
            .body(dbg!(point.to_line_protocol(precision)?))?)
    }
}

// curl --request POST \
// "http://localhost:8086/api/v2/write?org=YOUR_ORG&bucket=YOUR_BUCKET&precision=ns" \
// --header "Authorization: Token YOUR_API_TOKEN" \
// --header "Content-Type: text/plain; charset=utf-8" \
// --header "Accept: application/json" \
// --data-binary '
// airSensors,sensor_id=TLM0201 temperature=73.97038159354763,humidity=35.23103248356096,co=0.48445310567793615 1630424257000000000
// airSensors,sensor_id=TLM0202 temperature=75.30007505999716,humidity=35.651929918691714,co=0.5141876544505826 1630424257000000000
// '

#[derive(Copy, Clone)]
pub enum WritePrecision {
    NS,
    US,
    MS,
    S,
}

impl Default for WritePrecision {
    fn default() -> Self {
        Self::NS
    }
}

impl Display for WritePrecision {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            WritePrecision::NS => write!(f, "ns"),
            WritePrecision::US => write!(f, "us"),
            WritePrecision::MS => write!(f, "ms"),
            WritePrecision::S => write!(f, "s"),
        }
    }
}

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("")]
    TimeConversionError(String),
    #[error("Datapoints must have at least one field")]
    MissingField,
}

pub enum Authorization {
    Token(HeaderValue),
}

impl Authorization {
    pub(crate) fn header_value(&self) -> &HeaderValue {
        match self {
            Authorization::Token(header_value) => header_value,
        }
    }
}

impl Authorization {
    pub fn token(token: impl Into<String>) -> Result<Self, InvalidHeaderValue> {
        Ok(Self::Token(HeaderValue::try_from(
            "Token ".to_owned() + &token.into(),
        )?))
    }
}

impl From<Authorization> for (HeaderName, HeaderValue) {
    fn from(value: Authorization) -> Self {
        match value {
            Authorization::Token(header_value) => (header::AUTHORIZATION, header_value),
        }
    }
}

#[derive(Error, Debug)]
enum HttpClientError<E> {
    #[error(transparent)]
    ReqwestError(#[from] E),
}
