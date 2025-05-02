pub fn cdc_pg_channel(dataset_name: &str) -> String {
    format!("cdc:{}", dataset_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_pg_channel() {
        let channel = cdc_pg_channel("test");
        assert_eq!(channel, "cdc:test");
    }
}
