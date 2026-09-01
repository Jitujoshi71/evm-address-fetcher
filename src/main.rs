use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn upload_to_release(tag: &str, file_name: &str) {
    println!("Uploading {} to GitHub Release '{}'...", file_name, tag);
    let status = Command::new("gh")
        .args(["release", "upload", tag, file_name, "--clobber"])
        .status();

    match status {
        Ok(s) if s.success() => {
            println!("Uploaded: {}", file_name);
            let _ = fs::remove_file(file_name);
        }
        _ => eprintln!("Failed to upload {}", file_name),
    }
}

// JSON-RPC Batching: 1 Call me 20 Blocks ka data ek sath mangna
async fn fetch_rpc_batch_blocks(
    client: &reqwest::Client,
    rpc_url: &str,
    blocks: Vec<u64>,
) -> Vec<String> {
    let mut addrs = Vec::new();
    let batch_payload: Vec<Value> = blocks
        .iter()
        .map(|&b| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [format!("0x{:x}", b), true],
                "id": b
            })
        })
        .collect();

    for _ in 0..3 {
        let resp = client.post(rpc_url).json(&batch_payload).send().await;
        if let Ok(res) = resp {
            if let Ok(json_arr) = res.json::<Vec<Value>>().await {
                for item in json_arr {
                    if let Some(txs) = item.get("result").and_then(|r| r.get("transactions")).and_then(|t| t.as_array()) {
                        for tx in txs {
                            if let Some(from) = tx.get("from").and_then(|v| v.as_str()) {
                                addrs.push(from.to_lowercase());
                            }
                            if let Some(to) = tx.get("to").and_then(|v| v.as_str()) {
                                addrs.push(to.to_lowercase());
                            }
                        }
                    }
                }
                return addrs;
            }
        }
        sleep(Duration::from_millis(300)).await;
    }
    addrs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let release_tag = env::var("RELEASE_TAG").ok();
    let chain = env::var("TARGET_CHAIN").unwrap_or_else(|_| "bnb".to_string());

    // High performance Public EVM RPC URLs
    let rpc_url = match chain.as_str() {
        "bnb" => "https://binance.llamarpc.com",
        "ethereum" => "https://cloudflare-eth.com",
        "polygon" => "https://polygon-rpc.com",
        "arbitrum" => "https://arb1.arbitrum.io/rpc",
        "base" => "https://mainnet.base.org",
        "optimism" => "https://mainnet.optimism.io",
        "avalanche_c" => "https://api.avax.network/ext/bc/C/rpc",
        _ => "https://binance.llamarpc.com",
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(45))
        .build()?;
    let client = Arc::new(client);
    let rpc_url = Arc::new(rpc_url.to_string());

    // Current block height check karein
    let block_res: Value = client
        .post(rpc_url.as_str())
        .json(&json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}))
        .send()
        .await?
        .json()
        .await?;

    let latest_hex = block_res["result"].as_str().unwrap_or("0x1");
    let max_block = u64::from_str_radix(latest_hex.trim_start_matches("0x"), 16).unwrap_or(40_000_000);

    println!("Target Chain: {} | RPC URL: {} | Max Block: {}", chain, rpc_url, max_block);

    let part_size: u64 = 1_000_000; // 1000000 blocks per CSV Part
    let mut current_block: u64 = 0;
    let mut part_num: u32 = 1;

    while current_block < max_block {
        let end_block = (current_block + part_size).min(max_block);
        println!("\n[Batch #{:04}] Processing blocks {} to {}...", part_num, current_block, end_block);

        // Group blocks into chunks of 20 for RPC JSON batching
        let mut chunks = Vec::new();
        let mut b = current_block;
        while b < end_block {
            let chunk_end = (b + 20).min(end_block);
            chunks.push((b..chunk_end).collect::<Vec<u64>>());
            b = chunk_end;
        }

        // Run 10 parallel async RPC workers concurrently
        let results = stream::iter(chunks)
            .map(|block_chunk| {
                let client = Arc::clone(&client);
                let rpc = Arc::clone(&rpc_url);
                tokio::spawn(async move {
                    fetch_rpc_batch_blocks(&client, &rpc, block_chunk).await
                })
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        let mut unique_set: HashSet<String> = HashSet::new();
        for res in results {
            if let Ok(addresses) = res {
                for addr in addresses {
                    unique_set.insert(addr);
                }
            }
        }

        if !unique_set.is_empty() {
            let file_name = format!("{}_addresses_part_{:04}.csv.gz", chain, part_num);
            let file = File::create(&file_name)?;
            let enc = GzEncoder::new(file, Compression::default());
            let mut wtr = csv::Writer::from_writer(enc);

            wtr.write_record(&["address"])?;
            for addr in &unique_set {
                wtr.write_record(&[addr])?;
            }
            wtr.flush()?;

            println!("Saved {} unique addresses -> {}", unique_set.len(), file_name);

            if let Some(tag) = release_tag.as_deref() {
                upload_to_release(tag, &file_name);
            }
        }

        current_block = end_block;
        part_num += 1;
    }

    println!("\nPipeline completed successfully!");
    Ok(())
}
