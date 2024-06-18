use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

#[test]
fn test_deserialize() {
    use fs_err as fs;

    let provider: SubstreamsProvider =
        toml::from_str(&fs::read_to_string("src/provider/example.toml").unwrap()).unwrap();

    assert_eq!(provider.url, "https://<ENDPOINT>");
    assert_eq!(provider.token, Some("<AUTH_TOKEN>".to_string()));
}
