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
