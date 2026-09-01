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
            println!("Successfully uploaded: {}", file_name);
            let _ = fs::remove_file(file_name);
        }
        _ => eprintln!("Failed to upload {}", file_name),
    }
}

// JSON-RPC Batching (50 Blocks per request for maximum throughput)
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
        sleep(Duration::from_millis(200)).await;
    }
    addrs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let release_tag = env::var("RELEASE_TAG").ok();
    let chain = env::var("TARGET_CHAIN").unwrap_or_else(|_| "bnb".to_string());

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
        .timeout(Duration::from_secs(60))
        .build()?;
    let client = Arc::new(client);
    let rpc_url = Arc::new(rpc_url.to_string());

    // Fetch chain height
    let block_res: Value = client
        .post(rpc_url.as_str())
        .json(&json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}))
        .send()
        .await?
        .json()
        .await?;

    let latest_hex = block_res["result"].as_str().unwrap_or("0x1");
    let max_block = u64::from_str_radix(latest_hex.trim_start_matches("0x"), 16).unwrap_or(42_000_000);

    println!("Target: {} | Total Chain Blocks: {}", chain.to_uppercase(), max_block);

    // EXACT 1,000,000 (1 Million) BLOCKS PER PART
    let million_step: u64 = 1_000_000;
    let mut current_block: u64 = 0;
    let mut part_num: u32 = 1;

    while current_block < max_block {
        let end_block = (current_block + million_step).min(max_block);
        println!("\n=======================================================");
        println!("[Part #{:03}] Extracting Blocks {} to {} (1 Million Blocks Chunk)", part_num, current_block, end_block);
        println!("=======================================================");

        // Split 1M blocks into 50-block micro-batches for parallel RPC calls
        let mut micro_chunks = Vec::new();
        let mut b = current_block;
        while b < end_block {
            let chunk_end = (b + 50).min(end_block);
            micro_chunks.push((b..chunk_end).collect::<Vec<u64>>());
            b = chunk_end;
        }

        let mut unique_set: HashSet<String> = HashSet::new();

        // 15 Concurrent RPC Workers
        let mut stream = stream::iter(micro_chunks)
            .map(|chunk| {
                let client = Arc::clone(&client);
                let rpc = Arc::clone(&rpc_url);
                tokio::spawn(async move {
                    fetch_rpc_batch_blocks(&client, &rpc, chunk).await
                })
            })
            .buffer_unordered(15);

        let mut processed_micro_batches = 0;
        while let Some(res) = stream.next().await {
            if let Ok(addresses) = res {
                for addr in addresses {
                    unique_set.insert(addr);
                }
            }
            processed_micro_batches += 1;
            if processed_micro_batches % 2000 == 0 {
                println!("Processed {}k / 1,000k blocks (Addresses in memory: {})", processed_micro_batches * 50 / 1000, unique_set.len());
            }
        }

        if !unique_set.is_empty() {
            let file_name = format!("{}_addresses_1M_part_{:03}.csv.gz", chain, part_num);
            let file = File::create(&file_name)?;
            let enc = GzEncoder::new(file, Compression::default());
            let mut wtr = csv::Writer::from_writer(enc);

            wtr.write_record(&["address"])?;
            for addr in &unique_set {
                wtr.write_record(&[addr])?;
            }
            wtr.flush()?;

            println!("1 Million Block Chunk Complete! Total Unique: {} -> {}", unique_set.len(), file_name);

            // Instant Upload to Release
            if let Some(tag) = release_tag.as_deref() {
                upload_to_release(tag, &file_name);
            }
        }

        current_block = end_block;
        part_num += 1;
    }

    println!("\nAll 1 Million Block Parts extracted and uploaded!");
    Ok(())
}
