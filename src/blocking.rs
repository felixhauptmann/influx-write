use anyhow::bail;
use http::{header, HeaderName, HeaderValue, Method, Uri};
use url::Url;

use crate::influx::LineProtocol;
use crate::{Authorization, Client, DataPoint, InfluxWriter, WritePrecision, API_ENDPOINT_V2};

#[cfg(feature = "reqwest-blocking")]
pub mod reqwest;

impl From<Authorization> for (HeaderName, HeaderValue) {
    fn from(value: Authorization) -> Self {
        match value {
            Authorization::Token(header_value) => (header::AUTHORIZATION, header_value),
        }
    }
}

pub trait BlockingClient {
    fn execute(&mut self, req: http::Request<String>) -> anyhow::Result<http::Response<Vec<u8>>>;
}

impl<T> Client for T where T: BlockingClient {}

impl<W: BlockingClient> InfluxWriter<W> {
    pub fn new_with_client(
        client: W,
        url: Url,
        authorization: Authorization,
        org: impl Into<String>,
        bucket: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            url: url.join(API_ENDPOINT_V2)?,
            authorization,
            org: org.into(),
            bucket: bucket.into(),
        })
    }

    pub fn write_single(&mut self, point: DataPoint) -> anyhow::Result<()> {
        self.write(vec![point])
    }

    /// Write point with specified precision
    pub fn write_single_with_precision(
        &mut self,
        point: DataPoint,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        self.write_with_precision(vec![point], precision)
    }

    /// Write point with default precision
    pub fn write(&mut self, point: impl IntoIterator<Item = DataPoint>) -> anyhow::Result<()> {
        self.write_with_precision(point, WritePrecision::default())
    }

    /// Write point with specified precision
    pub fn write_with_precision(
        &mut self,
        point: impl IntoIterator<Item = DataPoint>,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        let mut url = self.url.clone();
        url.query_pairs_mut()
            .extend_pairs([("org", &self.org), ("bucket", &self.bucket)]);

        let req = http::request::Builder::new()
            .uri(Uri::try_from(url.as_str())?)
            .header(header::USER_AGENT, "influx-write/0.0.0")
            .header(header::AUTHORIZATION, self.authorization.header_value())
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(header::ACCEPT, "application/json")
            .method(Method::POST)
            .body(point.to_line_protocol(precision)?)?;

        let response = self.client.execute(req)?;

        if response.status().is_success() {
            Ok(())
        } else {
            bail!(
                "Got response: {:?}",
                String::from_utf8(response.body().clone())
            )
        }
    }
}
