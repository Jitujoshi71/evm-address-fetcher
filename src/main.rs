use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::{copy, Cursor};
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

fn upload_to_release(tag: &str, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Uploading {} to release tag '{}'...", file_name, tag);
    let status = Command::new("gh")
        .args(["release", "upload", tag, file_name, "--clobber"])
        .status()?;

    if status.success() {
        println!("Uploaded successfully: {}", file_name);
        let _ = fs::remove_file(file_name);
    }
    Ok(())
}

fn fetch_batch(
    client: &reqwest::blocking::Client,
    api_key: &str,
    release_tag: Option<&str>,
    chain: &str,
    start_block: u64,
    end_block: u64,
    batch_num: u32,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!("\n[{}] Batch #{}: Blocks {} to {}", chain.to_uppercase(), batch_num, start_block, end_block);

    let sql_query = format!(
        "SELECT DISTINCT address FROM (SELECT \"from\" AS address FROM {}.transactions WHERE block_number >= {} AND block_number < {} UNION ALL SELECT \"to\" AS address FROM {}.transactions WHERE block_number >= {} AND block_number < {}) AS t WHERE address IS NOT NULL",
        chain, start_block, end_block, chain, start_block, end_block
    );

    let payload = serde_json::json!({ "sql": sql_query });

    let res = client
        .post("https://api.dune.com/api/v1/sql/execute")
        .header("X-DUNE-API-KEY", api_key)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()?;

    let resp_val: Value = res.json()?;
    let execution_id = match resp_val.get("execution_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => {
            eprintln!("Execution submit failed: {:?}", resp_val);
            return Ok(false);
        }
    };

    let status_url = format!("https://api.dune.com/api/v1/execution/{}/status", execution_id);
    let mut is_completed = false;

    for _ in 0..60 {
        sleep(Duration::from_secs(6));
        let res = match client.get(&status_url).header("X-DUNE-API-KEY", api_key).send() {
            Ok(r) => r,
            Err(_) => continue,
        };

        if let Ok(json_res) = res.json::<Value>() {
            if let Some(state) = json_res.get("state").and_then(|s| s.as_str()) {
                if state == "QUERY_STATE_COMPLETED" {
                    is_completed = true;
                    break;
                } else if state == "QUERY_STATE_FAILED" || state == "QUERY_STATE_CANCELLED" {
                    return Ok(false);
                }
            }
        }
        print!(".");
    }

    if !is_completed {
        eprintln!("\nBatch #{} timed out.", batch_num);
        return Ok(false);
    }

    let csv_url = format!("https://api.dune.com/api/v1/execution/{}/results/csv", execution_id);
    let mut csv_resp = client.get(&csv_url).header("X-DUNE-API-KEY", api_key).send()?;

    let file_name = format!("{}_addresses_part_{:04}.csv.gz", chain, batch_num);
    let file = File::create(&file_name)?;
    let mut encoder = GzEncoder::new(file, Compression::default());

    let mut content = Vec::new();
    csv_resp.copy_to(&mut content)?;
    let mut cursor = Cursor::new(content);
    copy(&mut cursor, &mut encoder)?;
    encoder.finish()?;

    println!("\nSaved {}", file_name);

    if let Some(tag) = release_tag {
        let _ = upload_to_release(tag, &file_name);
    }

    Ok(true)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY required");
    let release_tag = env::var("RELEASE_TAG").ok();
    let chain = env::var("TARGET_CHAIN").unwrap_or_else(|_| "bnb".to_string());

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;

    let (block_step, max_blocks) = match chain.as_str() {
        "ethereum" => (1_000_000, 23_000_000),
        "polygon" => (1_000_000, 68_000_000),
        "arbitrum" => (2_000_000, 300_000_000),
        "base" => (1_000_000, 25_000_000),
        "optimism" => (1_000_000, 130_000_000),
        "avalanche_c" => (1_000_000, 55_000_000),
        _ => (1_000_000, 42_000_000), // BNB
    };

    let mut current_block: u64 = 0;
    let mut batch_counter: u32 = 1;

    println!("Starting {} pipeline (Step: {}, Max: {})...", chain, block_step, max_blocks);

    while current_block < max_blocks {
        let next_block = (current_block + block_step).min(max_blocks);
        let _ = fetch_batch(&client, &api_key, release_tag.as_deref(), &chain, current_block, next_block, batch_counter);
        current_block = next_block;
        batch_counter += 1;
        sleep(Duration::from_secs(3));
    }

    println!("\nExtraction finished!");
    Ok(())
}
