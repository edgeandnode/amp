/// A wrapper that redacts its contents in `Debug` output.
///
/// This type is useful for wrapping sensitive data like URLs with API keys,
/// authentication tokens, or passwords. The inner value can be accessed
/// transparently via `Deref`, but when printed using `Debug`, it will
/// display `<redacted>` instead of the actual value.
///
/// # Important Security Note
///
/// This type does **not** implement `Serialize` to prevent accidental
/// serialization of sensitive data. It only implements `Deserialize`.
#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Redacted<T>(T);

impl<T> From<T> for Redacted<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Redacted<T> {
    /// Consumes the wrapper and returns the inner value.
    ///
    /// This follows standard Rust newtype conventions (like `Arc::into_inner`, `Mutex::into_inner`)
    /// and avoids unnecessary clones when consuming the wrapper.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Redacted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for Redacted<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> std::borrow::Borrow<T> for Redacted<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl<T> std::fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl<'de, T> serde::Deserialize<'de> for Redacted<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Redacted)
    }
}

#[cfg(feature = "schemars")]
impl<T> schemars::JsonSchema for Redacted<T>
where
    T: schemars::JsonSchema,
{
    fn inline_schema() -> bool {
        T::inline_schema()
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        T::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        T::schema_id()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        T::json_schema(generator)
    }

    fn _schemars_private_non_optional_json_schema(
        generator: &mut schemars::SchemaGenerator,
    ) -> schemars::Schema {
        T::_schemars_private_non_optional_json_schema(generator)
    }

    fn _schemars_private_is_option() -> bool {
        T::_schemars_private_is_option()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use super::*;

    #[test]
    fn debug_with_string_prints_redacted() {
        //* Given
        let redacted = Redacted::from("sensitive-data");

        //* When
        let debug_output = format!("{:?}", redacted);

        //* Then
        assert_eq!(
            debug_output, "<redacted>",
            "debug output should show <redacted> instead of actual value"
        );
    }

    #[test]
    fn debug_with_integer_prints_redacted() {
        //* Given
        let redacted = Redacted::from(12345);

        //* When
        let debug_output = format!("{:?}", redacted);

        //* Then
        assert_eq!(
            debug_output, "<redacted>",
            "debug output should show <redacted> for integer values"
        );
    }

    #[test]
    fn deref_with_string_provides_transparent_access() {
        //* Given
        let redacted = Redacted::from("test-value".to_string());

        //* When
        let inner_value = &*redacted;

        //* Then
        assert_eq!(
            inner_value, "test-value",
            "deref should provide access to inner value"
        );
        assert_eq!(
            redacted.len(),
            10,
            "deref should provide transparent access to String methods"
        );
    }

    #[test]
    fn as_ref_with_string_returns_reference() {
        //* Given
        let redacted = Redacted::from("test-value".to_string());

        //* When
        let reference: &String = redacted.as_ref();

        //* Then
        assert_eq!(
            reference, "test-value",
            "as_ref should return reference to inner value"
        );
    }

    #[test]
    fn borrow_with_string_returns_borrowed_reference() {
        //* Given
        let redacted = Redacted::from("test-value".to_string());

        //* When
        let borrowed: &String = redacted.borrow();

        //* Then
        assert_eq!(
            borrowed, "test-value",
            "borrow should return borrowed reference to inner value"
        );
    }

    #[test]
    fn clone_with_string_creates_equal_copy() {
        //* Given
        let original = Redacted::from("test-value".to_string());

        //* When
        let cloned = original.clone();

        //* Then
        assert_eq!(original, cloned, "cloned value should equal original");
    }

    #[test]
    fn deserialize_from_json_with_string_succeeds() {
        //* Given
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct TestConfig {
            secret: Redacted<String>,
        }

        let json = r#"{"secret": "my-secret-value"}"#;

        //* When
        let config: TestConfig =
            serde_json::from_str(json).expect("deserialization should succeed with valid JSON");

        //* Then
        assert_eq!(
            &*config.secret, "my-secret-value",
            "deserialized value should match original"
        );
        assert_eq!(
            format!("{:?}", config.secret),
            "<redacted>",
            "debug output should show <redacted> after deserialization"
        );
    }

    #[test]
    fn deserialize_from_json_with_url_succeeds() {
        //* Given
        #[derive(serde::Deserialize)]
        struct Config {
            endpoint: Redacted<url::Url>,
        }

        let json = r#"{"endpoint": "https://example.com/api?key=secret123"}"#;

        //* When
        let config: Config =
            serde_json::from_str(json).expect("URL deserialization should succeed with valid JSON");

        //* Then
        assert_eq!(
            config.endpoint.scheme(),
            "https",
            "deserialized URL should have correct scheme"
        );
        assert_eq!(
            config.endpoint.host_str(),
            Some("example.com"),
            "deserialized URL should have correct host"
        );
        assert_eq!(
            format!("{:?}", config.endpoint),
            "<redacted>",
            "debug output should show <redacted> for URL with sensitive query params"
        );
    }
}
