use reqwest::{Client, ClientBuilder, Request, Response};
use url::Url;

use crate::{AsyncClient, Authorization, HttpClientError, InfluxWriter};

pub struct ReqwestClient {
    client: Client,
}

impl ReqwestClient {
    pub(crate) fn new() -> Result<Self, HttpClientError<reqwest::Error>> {
        Ok(Self {
            client: ClientBuilder::new().build()?,
        })
    }
}

impl AsyncClient for ReqwestClient {
    async fn execute(
        &mut self,
        req: http::Request<String>,
    ) -> anyhow::Result<http::Response<Vec<u8>>> {
        let response = self.client.execute(convert_request(req)?).await?;
        convert_response(response).await
    }
}

impl InfluxWriter<ReqwestClient> {
    pub fn new(
        url: Url,
        authorization: Authorization,
        org: impl Into<String>,
        bucket: impl Into<String>,
    ) -> anyhow::Result<Self> {
        Self::new_with_client(ReqwestClient::new()?, url, authorization, org, bucket)
    }
}

fn convert_request<T>(
    req: http::Request<T>,
) -> Result<Request, <Request as TryFrom<http::Request<T>>>::Error>
where
    Request: TryFrom<http::Request<T>>,
{
    Request::try_from(req)
}

async fn convert_response(resp: Response) -> anyhow::Result<http::Response<Vec<u8>>> {
    let mut response = http::response::Builder::new();

    response.headers_mut().unwrap().extend(
        resp.headers()
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    Ok(response.body(resp.bytes().await?.to_vec())?)
}
