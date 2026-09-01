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

// JSON-RPC Batching with Multi-RPC Retry
async fn fetch_rpc_batch_blocks(
    client: &reqwest::Client,
    rpc_urls: &[String],
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

    for rpc in rpc_urls {
        for _ in 0..2 {
            let resp = client.post(rpc).json(&batch_payload).send().await;
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
                    if !addrs.is_empty() || blocks.is_empty() {
                        return addrs;
                    }
                }
            }
            sleep(Duration::from_millis(150)).await;
        }
    }
    addrs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let release_tag = env::var("RELEASE_TAG").ok();
    let chain = env::var("TARGET_CHAIN").unwrap_or_else(|_| "bnb".to_string());

    // Verified High-Availability Official & Public RPCs
    let rpc_list: Vec<String> = match chain.as_str() {
        "bnb" => vec![
            "https://bsc-dataseed.binance.org/".into(),
            "https://bsc-dataseed1.defibit.io/".into(),
            "https://bsc-dataseed1.ninicoin.io/".into(),
            "https://rpc.ankr.com/bsc".into(),
            "https://1rpc.io/bnb".into(),
        ],
        "ethereum" => vec![
            "https://eth.llamarpc.com".into(),
            "https://cloudflare-eth.com".into(),
            "https://rpc.ankr.com/eth".into(),
            "https://1rpc.io/eth".into(),
        ],
        "polygon" => vec![
            "https://polygon-rpc.com".into(),
            "https://rpc.ankr.com/polygon".into(),
            "https://1rpc.io/matic".into(),
        ],
        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc".into(),
            "https://rpc.ankr.com/arbitrum".into(),
            "https://1rpc.io/arb".into(),
        ],
        "base" => vec![
            "https://mainnet.base.org".into(),
            "https://base.llamarpc.com".into(),
            "https://1rpc.io/base".into(),
        ],
        "optimism" => vec![
            "https://mainnet.optimism.io".into(),
            "https://optimism.llamarpc.com".into(),
            "https://1rpc.io/op".into(),
        ],
        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc".into(),
            "https://rpc.ankr.com/avalanche".into(),
            "https://1rpc.io/avax/c".into(),
        ],
        _ => vec!["https://bsc-dataseed.binance.org/".into()],
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let client = Arc::new(client);
    let rpc_list = Arc::new(rpc_list);

    // Latest block height check with fallback
    let mut max_block = 42_000_000;
    for rpc in rpc_list.iter() {
        if let Ok(resp) = client
            .post(rpc)
            .json(&json!({"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}))
            .send()
            .await
        {
            if let Ok(block_res) = resp.json::<Value>().await {
                if let Some(hex_str) = block_res["result"].as_str() {
                    if let Ok(b) = u64::from_str_radix(hex_str.trim_start_matches("0x"), 16) {
                        max_block = b;
                        println!("Connected successfully to RPC: {} | Height: {}", rpc, max_block);
                        break;
                    }
                }
            }
        }
    }

    println!("Target: {} | Max Chain Height: {}", chain.to_uppercase(), max_block);

    // 1,000,000 (1 Million) Blocks per Part
    let million_step: u64 = 1_000_000;
    let mut current_block: u64 = 0;
    let mut part_num: u32 = 1;

    while current_block < max_block {
        let end_block = (current_block + million_step).min(max_block);
        println!("\n=======================================================");
        println!("[Part #{:03}] Extracting Blocks {} to {} (1 Million Chunk)", part_num, current_block, end_block);
        println!("=======================================================");

        let mut micro_chunks = Vec::new();
        let mut b = current_block;
        while b < end_block {
            let chunk_end = (b + 25).min(end_block); // Safe 25-block micro batches
            micro_chunks.push((b..chunk_end).collect::<Vec<u64>>());
            b = chunk_end;
        }

        let mut unique_set: HashSet<String> = HashSet::new();

        let mut stream = stream::iter(micro_chunks)
            .map(|chunk| {
                let client = Arc::clone(&client);
                let rpcs = Arc::clone(&rpc_list);
                tokio::spawn(async move {
                    fetch_rpc_batch_blocks(&client, &rpcs, chunk).await
                })
            })
            .buffer_unordered(12);

        let mut processed_batches = 0;
        while let Some(res) = stream.next().await {
            if let Ok(addresses) = res {
                for addr in addresses {
                    unique_set.insert(addr);
                }
            }
            processed_batches += 1;
            if processed_batches % 2000 == 0 {
                println!(
                    "Progress: {}k / 1,000k blocks scanned (Addresses in chunk: {})",
                    processed_batches * 25 / 1000,
                    unique_set.len()
                );
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

            println!("Chunk #{} completed! Total: {} -> {}", part_num, unique_set.len(), file_name);

            if let Some(tag) = release_tag.as_deref() {
                upload_to_release(tag, &file_name);
            }
        }

        current_block = end_block;
        part_num += 1;
    }

    println!("\nAll parts extracted successfully!");
    Ok(())
}
