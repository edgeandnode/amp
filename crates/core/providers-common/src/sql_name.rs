/// Sanitize a string to produce a SQL-safe identifier (for schema or catalog names).
///
/// The sanitization rules are:
/// 1. Convert to lowercase
/// 2. Replace non-alphanumeric characters with `_`
/// 3. Collapse repeated `_` into a single `_`
/// 4. Trim leading and trailing `_`
/// 5. Prefix `_` if the name starts with a digit
///
/// This function is used to normalize network IDs into SQL-safe schema names.
///
/// This function only preserves ASCII alphanumeric characters. Non-ASCII characters
/// (such as accented letters, CJK characters, etc.) are silently converted to underscores.
/// If this results in an empty string after trimming, the caller must handle this as an error.
pub fn sanitize_sql_name(name: &str) -> String {
    // Step 1: Lowercase
    let mut result = name.to_lowercase();

    // Step 2: Replace non-alphanumeric with _
    result = result
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();

    // Step 3: Collapse repeated underscores
    while result.contains("__") {
        result = result.replace("__", "_");
    }

    // Step 4: Trim leading and trailing underscores
    result = result.trim_matches('_').to_string();

    // Step 5: Prefix _ if starts with digit
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert(0, '_');
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_sql_name_with_hyphens_replaces_with_underscores() {
        //* Given
        let input = "arbitrum-one";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "arbitrum_one",
            "hyphens should be replaced with underscores"
        );
    }

    #[test]
    fn sanitize_sql_name_with_uppercase_converts_to_lowercase() {
        //* Given
        let input = "Mainnet";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(result, "mainnet", "should convert uppercase to lowercase");
    }

    #[test]
    fn sanitize_sql_name_with_lowercase_alphanumeric_preserves_input() {
        //* Given
        let input = "base";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(result, "base", "lowercase alphanumeric should be preserved");
    }

    #[test]
    fn sanitize_sql_name_with_double_underscores_collapses_to_single() {
        //* Given
        let input = "base__v2";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "base_v2",
            "consecutive underscores should collapse to single"
        );
    }

    #[test]
    fn sanitize_sql_name_with_triple_underscores_collapses_to_single() {
        //* Given
        let input = "a___b";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "a_b",
            "multiple consecutive underscores should collapse to single"
        );
    }

    #[test]
    fn sanitize_sql_name_with_leading_and_trailing_underscores_trims_them() {
        //* Given
        let input = "____test____";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "test",
            "leading and trailing underscores should be trimmed"
        );
    }

    #[test]
    fn sanitize_sql_name_with_boundary_underscores_trims_them() {
        //* Given
        let input = "__test__";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(result, "test", "boundary underscores should be trimmed");
    }

    #[test]
    fn sanitize_sql_name_with_single_boundary_underscores_trims_them() {
        //* Given
        let input = "_network_";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "network",
            "single boundary underscores should be trimmed"
        );
    }

    #[test]
    fn sanitize_sql_name_with_leading_digit_prefixes_underscore() {
        //* Given
        let input = "42-network";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "_42_network",
            "names starting with digit should be prefixed with underscore"
        );
    }

    #[test]
    fn sanitize_sql_name_with_numeric_only_input_prefixes_underscore() {
        //* Given
        let input = "2023";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "_2023",
            "numeric-only names should be prefixed with underscore"
        );
    }

    #[test]
    fn sanitize_sql_name_with_empty_string_returns_empty() {
        //* When
        let result = sanitize_sql_name("");

        //* Then
        assert_eq!(result, "", "empty string should return empty");
    }

    #[test]
    fn sanitize_sql_name_with_only_underscores_returns_empty() {
        //* Given
        let input = "___";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "",
            "underscores-only input should return empty after trimming"
        );
    }

    #[test]
    fn sanitize_sql_name_with_only_special_chars_returns_empty() {
        //* Given
        let input = "!!!";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "",
            "special characters should be converted to underscores and trimmed"
        );
    }

    #[test]
    fn sanitize_sql_name_with_mixed_case_and_hyphen_normalizes_correctly() {
        //* Given
        let input = "Test-Network_2024";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "test_network_2024",
            "mixed case and symbols should normalize to lowercase with underscores"
        );
    }

    #[test]
    fn sanitize_sql_name_with_double_colon_replaces_with_underscores() {
        //* Given
        let input = "Polygon::PoS";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "polygon_pos",
            "double colons should be replaced with underscores"
        );
    }

    #[test]
    fn sanitize_sql_name_with_multiple_symbols_normalizes_correctly() {
        //* Given
        let input = "ETH.Mainnet@v2";

        //* When
        let result = sanitize_sql_name(input);

        //* Then
        assert_eq!(
            result, "eth_mainnet_v2",
            "mixed symbols should be normalized to underscores"
        );
    }
}
