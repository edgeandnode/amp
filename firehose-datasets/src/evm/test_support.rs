use super::pbethereum;
use anyhow::Ok;
use fs_err as fs;

// Currently dead code, we keep it around in case we want to use this to generate human-readable test
// files to test `protobufs_to_rows` for example.
pub fn write_block_to_pb_json(
    path: &std::path::Path,
    block: &pbethereum::Block,
) -> Result<(), anyhow::Error> {
    fn file_writer(path: &std::path::Path) -> Result<fs::File, anyhow::Error> {
        let file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        file.set_len(0)?;
        Ok(file)
    }

    let writer = file_writer(path)?;

    // This is to get a hex representation for bytes arrays in the JSON.
    let mut json_block = serde_json::to_value(&block)?;
    replace_u8_arrays_with_hex_string(&mut json_block);

    serde_json::to_writer_pretty(writer, &json_block)?;
    Ok(())
}

fn replace_u8_arrays_with_hex_string(value: &mut serde_json::Value) {
    use serde_json::Value;

    fn is_u8(v: &Value) -> bool {
        use Value::Number;
        match v {
            Number(num) => num.as_u64().map_or(false, |u| u <= u8::MAX as u64),
            _ => false, // Not a number
        }
    }

    match value {
        Value::Object(map) => {
            for (_, v) in map {
                replace_u8_arrays_with_hex_string(v);
            }
        }
        Value::Array(vec) => {
            // 32 is hashes, 20 is addresses, 256 is logs bloom.
            if (vec.len() == 32 || vec.len() == 20 || vec.len() == 256) && vec.iter().all(is_u8) {
                // Convert the 32-byte array to a hex string.
                let bytes: Vec<u8> = vec.iter().map(|v| v.as_u64().unwrap() as u8).collect();
                let hex_str = format!("0x{}", hex::encode(bytes));
                *value = Value::String(hex_str);
            } else {
                for v in vec {
                    replace_u8_arrays_with_hex_string(v);
                }
            }
        }
        _ => {}
    }
}
