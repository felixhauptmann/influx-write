use std::vec;

use mockito::Matcher;

use influx_write::{Authorization, DataPointBuilder};

const MOCK_ORG: &str = "MyOrg";
const MOCK_BUCKET: &str = "MyBucket";
const MOCK_TOKEN: &str = "djw9r30ur9093ur";

#[cfg(feature = "reqwest-blocking")]
#[test]
fn test_reqwest_blocking() -> anyhow::Result<()> {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", influx_write::API_ENDPOINT_V2)
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("org".into(), MOCK_ORG.into()),
            Matcher::UrlEncoded("bucket".into(), MOCK_BUCKET.into()),
        ]))
        .match_header(
            "authorization",
            Matcher::Exact(format!("Token {MOCK_TOKEN}")),
        )
        .match_body(Matcher::Exact("measurement field=0".into()))
        .create();

    let mut client =
        influx_write::InfluxWriter::<influx_write::blocking::reqwest::ReqwestClient>::new(
            server.url().parse()?,
            Authorization::token(MOCK_TOKEN)?,
            MOCK_ORG,
            MOCK_BUCKET,
        )?;

    client.write_single_blocking(
        DataPointBuilder::new("measurement")
            .with_field("field", 0.)
            .into(),
    )?;

    mock.assert();

    Ok(())
}

#[cfg(feature = "reqwest")]
#[tokio::test]
async fn test_reqwest_async() -> anyhow::Result<()> {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("POST", influx_write::API_ENDPOINT_V2)
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("org".into(), MOCK_ORG.into()),
            Matcher::UrlEncoded("bucket".into(), MOCK_BUCKET.into()),
        ]))
        .match_header(
            "authorization",
            Matcher::Exact(format!("Token {MOCK_TOKEN}")),
        )
        .match_body(Matcher::Exact("measurement field=0".into()))
        .create();

    let mut client = influx_write::InfluxWriter::<influx_write::reqwest::ReqwestClient>::new(
        server.url().parse()?,
        Authorization::token(MOCK_TOKEN)?,
        MOCK_ORG,
        MOCK_BUCKET,
    )?;

    client
        .write_single(
            DataPointBuilder::new("measurement")
                .with_field("field", 0.)
                .into(),
        )
        .await?;

    mock.assert();

    Ok(())
}
