use anyhow::bail;
use url::Url;

use crate::{Authorization, DataPoint, InfluxWriter, WritePrecision, API_ENDPOINT_V2};

#[cfg(feature = "reqwest")]
pub mod reqwest;

pub trait AsyncClient {
    fn execute(
        &mut self,
        req: http::Request<String>,
    ) -> impl std::future::Future<Output = anyhow::Result<http::Response<Vec<u8>>>> + Send;
}

impl<W: AsyncClient> InfluxWriter<W> {
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

    pub async fn write_single(&mut self, point: DataPoint) -> anyhow::Result<()> {
        self.write(vec![point]).await
    }

    /// Write point with specified precision
    pub async fn write_single_with_precision(
        &mut self,
        point: DataPoint,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        self.write_with_precision(vec![point], precision).await
    }

    /// Write point with default precision
    pub async fn write(
        &mut self,
        points: impl IntoIterator<Item = DataPoint>,
    ) -> anyhow::Result<()> {
        self.write_with_precision(points, WritePrecision::default())
            .await
    }

    /// Write point with specified precision
    pub async fn write_with_precision(
        &mut self,
        points: impl IntoIterator<Item = DataPoint>,
        precision: WritePrecision,
    ) -> anyhow::Result<()> {
        let req = self.build_request(points, precision)?;

        let response = self.client.execute(req).await?;

        if response.status().is_success() && response.body() == b"" {
            Ok(())
        } else {
            bail!(
                "Got response: {:?}",
                String::from_utf8(response.body().clone())
            )
        }
    }
}
