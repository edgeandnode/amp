use std::{collections::BTreeMap, path::Path};

use alloy::json_abi::JsonAbi;
use anyhow::{anyhow, Context as _};
use common::manifest::{self, Manifest};
use indoc::formatdoc;

use super::nozzle::Nozzle;

pub async fn load_manifests(dir: &Path, nozzle: &Nozzle) -> anyhow::Result<Vec<Manifest>> {
    let mut manifests: Vec<Manifest> = Default::default();
    for entry in std::fs::read_dir(dir).context(anyhow!("read {}", dir.display()))? {
        let entry = entry.context(anyhow!("read {}", dir.display()))?;
        let metadata = entry
            .metadata()
            .context(anyhow!("read {}", entry.path().display()))?;
        if !(metadata.is_dir() && entry.file_name().to_string_lossy().ends_with(".sol")) {
            continue;
        }
        let dir = entry.path();
        for entry in std::fs::read_dir(&dir).context(anyhow!("read {}", dir.display()))? {
            let entry = entry.context(anyhow!("read {}", dir.display()))?;
            let metadata = entry
                .metadata()
                .context(anyhow!("read {}", entry.path().display()))?;
            if !(metadata.is_file() && entry.file_name().to_string_lossy().ends_with(".json")) {
                continue;
            }
            let dataset =
                camelcase_to_snakecase(&entry.file_name().to_string_lossy().replace(".json", ""));
            if manifests.iter().any(|m| m.name == dataset) {
                log::warn!(
                    "skipping duplicate dataset from path {}",
                    entry.path().display()
                );
                continue;
            }
            let abi = load_contract_abi(&entry.path())?;
            let events = match filter_abi_events(&abi) {
                Ok(events) => events,
                Err(err) => {
                    log::info!("skipping contract dataset '{dataset}': {err}");
                    continue;
                }
            };
            let mut tables: BTreeMap<String, manifest::Table> = Default::default();
            for event in events {
                let table = camelcase_to_snakecase(&event.name);
                log::info!(
                    "adding table {dataset}.{table} for {}",
                    event.full_signature(),
                );
                let sql = sql_for_event(&event.full_signature());
                let schema = nozzle.schema(&sql).await?;
                let input = manifest::TableInput::View(manifest::View { sql });
                tables.insert(table, manifest::Table { input, schema });
            }
            manifests.push(Manifest {
                name: dataset,
                version: semver::Version::new(0, 0, 0),
                dependencies: [(
                    "anvil".into(),
                    manifest::Dependency {
                        owner: "".into(),
                        name: "anvil".into(),
                        version: "*".parse().unwrap(),
                    },
                )]
                .into(),
                tables,
            });
        }
    }
    Ok(manifests)
}

fn sql_for_event(signature: &str) -> String {
    formatdoc! {r#"
        SELECT
            l.block_num,
            l.timestamp,
            l.address,
            evm_decode(l.topic1, l.topic2, l.topic3, l.data, '{signature}') AS event
        FROM
            anvil.logs l
        WHERE
            l.topic0 = evm_topic('{signature}')
    "#}
}

fn load_contract_abi(file: &Path) -> anyhow::Result<JsonAbi> {
    #[derive(serde::Deserialize)]
    struct ContractArtifact {
        abi: JsonAbi,
    }
    let file = std::fs::File::open(file).context(anyhow!("read {}", file.display()))?;
    let artifact: ContractArtifact =
        serde_json::from_reader(&file).context("parse contract artifact")?;
    Ok(artifact.abi)
}

fn filter_abi_events(abi: &JsonAbi) -> anyhow::Result<Vec<&alloy::json_abi::Event>> {
    // Nozzle dump seems to fail if any events in a dataset contain a type that cannot be converted
    // to an arrow data type.
    for event in abi.events() {
        for input_type in event.inputs.iter().map(|i| i.selector_type()) {
            anyhow::ensure!(
                !input_type.ends_with("[]"),
                "unsupported event: {}: unsupported type {}",
                event.full_signature().replace("event ", ""),
                input_type
            );
        }
    }
    anyhow::ensure!(!abi.events.is_empty(), "no events");
    Ok(abi.events().collect())
}

fn camelcase_to_snakecase(name: &str) -> String {
    let mut result = String::new();
    let mut prev_char_was_upper = false;
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() {
            if (i != 0) && !prev_char_was_upper {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
            prev_char_was_upper = true;
        } else {
            result.push(c);
            prev_char_was_upper = false;
        }
    }
    result
}

#[cfg(test)]
mod test {
    #[test]
    fn camelcase_to_snakecase() {
        let tests = [
            ("IERC721Enumerable", "ierc721_enumerable"),
            ("MockERC721", "mock_erc721"),
            ("stdStorageSafe", "std_storage_safe"),
            ("Vm", "vm"),
            ("VmSafe", "vm_safe"),
        ];
        for (input, expected) in tests {
            assert_eq!(&super::camelcase_to_snakecase(input), expected);
        }
    }
}
