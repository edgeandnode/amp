use common::store::Store;
use firehose_datasets::Error;

use serde::Deserialize;

pub const DATASET_KIND: &str = "substreams";

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub provider: String,

    /// Substreams package manifest URL
    pub manifest: String,

    /// Substreams output module name
    pub module: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

pub(crate) async fn extract_def_and_provider(
    dataset_def: toml::Value,
    provider_store: &Store,
) -> Result<(DatasetDef, SubstreamsProvider), Error> {
    let def: DatasetDef = dataset_def.try_into()?;
    let provider_bytes = provider_store.get_bytes(def.provider.as_str()).await?;
    let provider_string = String::from_utf8(provider_bytes.to_vec())
        .map_err(|_| Error::DatasetDefinitionError("provider file is not utf8".into()))?;
    let provider: SubstreamsProvider = toml::from_str(&provider_string)?;

    // Consistency check: Kind is correct.
    if def.kind != DATASET_KIND {
        return Err(Error::DatasetDefinitionError(
            format!("expected dataset kind '{DATASET_KIND}', got '{}'", def.kind).into(),
        ));
    }

    Ok((def, provider))
}

#[tokio::test]
async fn test_deserialize() {
    let dataset_store = Store::new("example_config/dataset_defs/".to_string()).unwrap();

    let dataset_def: toml::Value = toml::from_str(
        &String::from_utf8(
            dataset_store
                .get_bytes("dataset_example.toml")
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap(),
    )
    .unwrap();

    let provider_store = Store::new("example_config/providers/".to_string()).unwrap();

    let (def, provider) = extract_def_and_provider(dataset_def, &provider_store)
        .await
        .unwrap();

    assert_eq!(provider.url, "https://<ENDPOINT>");
    assert_eq!(provider.token, Some("<AUTH_TOKEN>".to_string()));
    assert_eq!(def.module, "map_events");
    assert_eq!(def.manifest, "https://spkg.io/<package>");
}
