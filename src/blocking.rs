use anyhow::bail;
use url::Url;

use crate::{Authorization, DataPoint, InfluxWriter, WritePrecision, API_ENDPOINT_V2};

#[cfg(feature = "reqwest-blocking")]
pub mod reqwest;

pub trait BlockingClient {
    fn execute(&mut self, req: http::Request<String>) -> anyhow::Result<http::Response<Vec<u8>>>;
}

impl<W: BlockingClient> InfluxWriter<W> {
    pub fn new_with_blocking_client(
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

    pub fn write_single_blocking(&mut self, point: DataPoint) -> anyhow::Result<()> {
        self.write_blocking(vec![point])
    }

    /// Write point with specified precision
    pub fn write_single_with_precision_blocking(
        &mut self,
        point: DataPoint,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        self.write_with_precision_blocking(vec![point], precision)
    }

    /// Write point with default precision
    pub fn write_blocking(
        &mut self,
        points: impl IntoIterator<Item = DataPoint>,
    ) -> anyhow::Result<()> {
        self.write_with_precision_blocking(points, WritePrecision::default())
    }

    /// Write point with specified precision
    pub fn write_with_precision_blocking(
        &mut self,
        points: impl IntoIterator<Item = DataPoint>,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        let req = self.build_request(points, precision)?;

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
