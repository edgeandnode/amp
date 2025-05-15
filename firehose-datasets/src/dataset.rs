use common::store::Store;
use serde::Deserialize;

use crate::Error;

pub const DATASET_KIND: &str = "firehose";

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub provider: String,
    pub network: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
    pub network: String,
}

pub(crate) async fn extract_provider(
    dataset_def: toml::Value,
    provider_store: &Store,
) -> Result<FirehoseProvider, Error> {
    let def: DatasetDef = dataset_def.try_into()?;
    let provider_bytes = provider_store.get_bytes(def.provider.as_str()).await?;
    let provider_string = String::from_utf8(provider_bytes.to_vec())
        .map_err(|_| Error::DatasetDefinitionError("provider file is not utf8".into()))?;
    let provider: FirehoseProvider = toml::from_str(&provider_string)?;

    // Consistency checks: Kind is correct, networks match.
    if def.kind != DATASET_KIND {
        return Err(Error::DatasetDefinitionError(
            format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into(),
        ));
    }
    if provider.network != def.network {
        return Err(Error::DatasetDefinitionError(
            format!(
                "firehose network mismatch, provider has network '{}' but dataset has network '{}'",
                provider.network, def.network
            )
            .into(),
        ));
    }

    Ok(provider)
}

#[tokio::test]
async fn test_deserialize() {
    let dataset_store = Store::new("example_config/dataset_defs/".to_string(), None).unwrap();

    let dataset_def: toml::Value = toml::from_str(
        &String::from_utf8(
            dataset_store
                .get_bytes("example_firehose.toml")
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap(),
    )
    .unwrap();

    let provider_store = Store::new("example_config/providers/".to_string(), None).unwrap();

    let provider = extract_provider(dataset_def, &provider_store)
        .await
        .unwrap();

    assert_eq!(provider.url, "https://<ENDPOINT>");
    assert_eq!(provider.token, Some("<AUTH_TOKEN>".to_string()));
    assert_eq!(provider.network, "mainnet");
}
