use flate2::write::GzEncoder;
use flate2::Compression;

use futures::stream::{self, StreamExt};

use serde_json::{json, Value};

use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;


// ============================================================
// CONFIG
// ============================================================

const BLOCK_CHUNK: u64 = 1_000_000;
const RPC_BATCH_SIZE: u64 = 25;
const CONCURRENCY: usize = 12;

const MAX_RPC_RETRIES: usize = 3;

const RPC_TIMEOUT_SECONDS: u64 = 45;


// ============================================================
// RPC URLS
// ============================================================

fn get_rpc_list(chain: &str) -> Vec<String> {

    // User supplied RPC gets priority.
    let mut list = Vec::new();

    if let Ok(custom_rpc) = env::var("RPC_URL") {
        let rpc = custom_rpc.trim().to_string();

        if !rpc.is_empty() {
            list.push(rpc);
        }
    }

    let defaults: Vec<String> = match chain {

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

        _ => vec![
            "https://bsc-dataseed.binance.org/".into(),
        ],
    };

    for rpc in defaults {
        if !list.contains(&rpc) {
            list.push(rpc);
        }
    }

    list
}


// ============================================================
// HEX → U64
// ============================================================

fn hex_to_u64(value: &str) -> Option<u64> {

    let value = value.trim_start_matches("0x");

    u64::from_str_radix(value, 16).ok()
}


// ============================================================
// SINGLE RPC CALL
// ============================================================

async fn rpc_call(
    client: &reqwest::Client,
    rpc_urls: &[String],
    payload: Value,
) -> Result<Value, String> {

    let mut last_error = String::from("unknown RPC error");

    for rpc in rpc_urls {

        for attempt in 1..=MAX_RPC_RETRIES {

            let response = client
                .post(rpc)
                .json(&payload)
                .send()
                .await;

            match response {

                Ok(resp) => {

                    let status = resp.status();

                    if !status.is_success() {

                        last_error = format!(
                            "RPC {} HTTP status {}",
                            rpc,
                            status
                        );

                    } else {

                        match resp.json::<Value>().await {

                            Ok(value) => {

                                if value.get("error").is_some() {

                                    last_error = format!(
                                        "RPC {} returned error: {}",
                                        rpc,
                                        value["error"]
                                    );

                                } else {

                                    return Ok(value);
                                }
                            }

                            Err(e) => {

                                last_error = format!(
                                    "RPC {} invalid JSON: {}",
                                    rpc,
                                    e
                                );
                            }
                        }
                    }
                }

                Err(e) => {

                    last_error = format!(
                        "RPC {} request error: {}",
                        rpc,
                        e
                    );
                }
            }

            if attempt < MAX_RPC_RETRIES {

                sleep(Duration::from_millis(
                    300 * attempt as u64
                ))
                .await;
            }
        }

        eprintln!(
            "RPC failed, trying next RPC: {}",
            rpc
        );
    }

    Err(last_error)
}


// ============================================================
// GET BLOCK TIMESTAMP
// ============================================================

async fn get_block_timestamp(
    client: &reqwest::Client,
    rpc_urls: &[String],
    block: u64,
) -> Result<u64, String> {

    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [
            format!("0x{:x}", block),
            false
        ],
        "id": 1
    });

    let response = rpc_call(
        client,
        rpc_urls,
        payload
    )
    .await?;

    let result = response
        .get("result")
        .ok_or("Missing result")?;

    let timestamp_hex = result
        .get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or("Missing block timestamp")?;

    hex_to_u64(timestamp_hex)
        .ok_or_else(|| "Invalid timestamp".to_string())
}


// ============================================================
// GET LATEST BLOCK
// ============================================================

async fn get_latest_block(
    client: &reqwest::Client,
    rpc_urls: &[String],
) -> Result<u64, String> {

    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    });

    let response = rpc_call(
        client,
        rpc_urls,
        payload
    )
    .await?;

    let result = response
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or("Missing latest block result")?;

    hex_to_u64(result)
        .ok_or_else(|| "Invalid latest block".to_string())
}


// ============================================================
// FIND FIRST BLOCK AT / AFTER TIMESTAMP
// ============================================================

async fn find_block_by_timestamp(
    client: &reqwest::Client,
    rpc_urls: &[String],
    target_timestamp: u64,
    mut low: u64,
    mut high: u64,
) -> Result<u64, String> {

    println!(
        "Searching block for timestamp {}...",
        target_timestamp
    );

    while low < high {

        let mid = low + (high - low) / 2;

        let timestamp = get_block_timestamp(
            client,
            rpc_urls,
            mid
        )
        .await?;

        println!(
            "Binary search: block={} timestamp={} target={}",
            mid,
            timestamp,
            target_timestamp
        );

        if timestamp < target_timestamp {

            low = mid + 1;

        } else {

            high = mid;
        }
    }

    Ok(low)
}


// ============================================================
// FETCH BLOCK BATCH
// ============================================================

async fn fetch_rpc_batch_blocks(
    client: &reqwest::Client,
    rpc_urls: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, String> {

    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let batch_payload: Vec<Value> = blocks
        .iter()
        .map(|&block| {

            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [
                    format!("0x{:x}", block),
                    true
                ],
                "id": block
            })
        })
        .collect();

    let mut last_error = String::new();

    for rpc in rpc_urls {

        for attempt in 1..=MAX_RPC_RETRIES {

            let response = client
                .post(rpc)
                .json(&batch_payload)
                .send()
                .await;

            match response {

                Ok(resp) => {

                    if !resp.status().is_success() {

                        last_error = format!(
                            "{} HTTP {}",
                            rpc,
                            resp.status()
                        );

                    } else {

                        match resp.json::<Vec<Value>>().await {

                            Ok(items) => {

                                let mut addresses = Vec::new();

                                let mut received_blocks =
                                    HashSet::<u64>::new();

                                for item in items {

                                    if item.get("error").is_some() {

                                        last_error = format!(
                                            "{} returned RPC error: {}",
                                            rpc,
                                            item["error"]
                                        );

                                        continue;
                                    }

                                    let block_number =
                                        item.get("result")
                                            .and_then(|r| r.get("number"))
                                            .and_then(|n| n.as_str())
                                            .and_then(hex_to_u64);

                                    if let Some(block_number) =
                                        block_number
                                    {
                                        received_blocks.insert(
                                            block_number
                                        );
                                    }

                                    if let Some(txs) =
                                        item.get("result")
                                            .and_then(|r| r.get("transactions"))
                                            .and_then(|t| t.as_array())
                                    {

                                        for tx in txs {

                                            if let Some(from) =
                                                tx.get("from")
                                                    .and_then(|v| v.as_str())
                                            {
                                                addresses.push(
                                                    from.to_ascii_lowercase()
                                                );
                                            }

                                            if let Some(to) =
                                                tx.get("to")
                                                    .and_then(|v| v.as_str())
                                            {
                                                addresses.push(
                                                    to.to_ascii_lowercase()
                                                );
                                            }
                                        }
                                    }
                                }

                                // IMPORTANT:
                                // Don't silently accept partial RPC response.
                                let expected =
                                    blocks.len();

                                if received_blocks.len() == expected {

                                    return Ok(addresses);
                                }

                                last_error = format!(
                                    "{} returned only {}/{} requested blocks",
                                    rpc,
                                    received_blocks.len(),
                                    expected
                                );
                            }

                            Err(e) => {

                                last_error = format!(
                                    "{} invalid batch JSON: {}",
                                    rpc,
                                    e
                                );
                            }
                        }
                    }
                }

                Err(e) => {

                    last_error = format!(
                        "{} request failed: {}",
                        rpc,
                        e
                    );
                }
            }

            if attempt < MAX_RPC_RETRIES {

                sleep(Duration::from_millis(
                    500 * attempt as u64
                ))
                .await;
            }
        }

        eprintln!(
            "Batch RPC failed: {}",
            last_error
        );
    }

    Err(last_error)
}


// ============================================================
// CHECKPOINT
// ============================================================

fn checkpoint_path() -> &'static str {
    "output/checkpoint.txt"
}


fn save_checkpoint(block: u64) -> Result<(), Box<dyn std::error::Error>> {

    let tmp = "output/checkpoint.tmp";

    fs::write(
        tmp,
        block.to_string()
    )?;

    fs::rename(
        tmp,
        checkpoint_path()
    )?;

    Ok(())
}


fn load_checkpoint() -> Option<u64> {

    if !Path::new(checkpoint_path()).exists() {
        return None;
    }

    let content =
        fs::read_to_string(checkpoint_path()).ok()?;

    content.trim().parse::<u64>().ok()
}


// ============================================================
// WRITE CSV.GZ
// ============================================================

fn write_addresses_file(
    chain: &str,
    part_num: u32,
    unique_set: &HashSet<String>,
) -> Result<String, Box<dyn std::error::Error>> {

    let file_name = format!(
        "output/{}_addresses_1M_part_{:03}.csv.gz",
        chain,
        part_num
    );

    let file = File::create(&file_name)?;

    let encoder =
        GzEncoder::new(
            file,
            Compression::default()
        );

    let mut writer =
        csv::Writer::from_writer(encoder);

    writer.write_record(["address"])?;

    for address in unique_set {

        writer.write_record([address])?;
    }

    writer.flush()?;

    let encoder =
        writer.into_inner()
            .map_err(|e| e.into_error())?;

    encoder.finish()?;

    Ok(file_name)
}


// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // --------------------------------------------------------
    // CLI ARGUMENTS
    // --------------------------------------------------------

    let args: Vec<String> =
        env::args().collect();

    let mut chain =
        env::var("TARGET_CHAIN")
            .unwrap_or_else(|_| "bnb".to_string());

    let mut start_ts =
        env::var("START_TIMESTAMP")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());

    let mut end_ts =
        env::var("END_TIMESTAMP")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());


    let mut i = 1;

    while i < args.len() {

        match args[i].as_str() {

            "--chain" => {

                i += 1;

                if i < args.len() {
                    chain = args[i].clone();
                }
            }

            "--start-ts" => {

                i += 1;

                if i < args.len() {

                    start_ts =
                        Some(
                            args[i].parse::<u64>()?
                        );
                }
            }

            "--end-ts" => {

                i += 1;

                if i < args.len() {

                    end_ts =
                        Some(
                            args[i].parse::<u64>()?
                        );
                }
            }

            _ => {}
        }

        i += 1;
    }


    let start_ts =
        start_ts.ok_or("START_TIMESTAMP missing")?;

    let end_ts =
        end_ts.ok_or("END_TIMESTAMP missing")?;


    if start_ts > end_ts {

        return Err(
            "Start timestamp is greater than end timestamp"
                .into()
        );
    }


    // --------------------------------------------------------
    // OUTPUT DIRECTORY
    // --------------------------------------------------------

    fs::create_dir_all("output")?;


    // --------------------------------------------------------
    // RPC
    // --------------------------------------------------------

    let rpc_list =
        get_rpc_list(&chain);

    println!();
    println!("=======================================================");
    println!("EVM ADDRESS EXTRACTOR");
    println!("=======================================================");
    println!("Chain       : {}", chain);
    println!("Start TS    : {}", start_ts);
    println!("End TS      : {}", end_ts);
    println!("RPC count   : {}", rpc_list.len());
    println!("=======================================================");

    for (index, rpc) in rpc_list.iter().enumerate() {

        println!(
            "RPC #{}: {}",
            index + 1,
            rpc
        );
    }


    // --------------------------------------------------------
    // HTTP CLIENT
    // --------------------------------------------------------

    let client = reqwest::Client::builder()
        .timeout(
            Duration::from_secs(
                RPC_TIMEOUT_SECONDS
            )
        )
        .pool_max_idle_per_host(32)
        .build()?;

    let client =
        Arc::new(client);

    let rpc_list =
        Arc::new(rpc_list);


    // --------------------------------------------------------
    // LATEST BLOCK
    // --------------------------------------------------------

    let latest_block =
        get_latest_block(
            &client,
            &rpc_list
        )
        .await?;

    println!();
    println!(
        "Latest chain block: {}",
        latest_block
    );


    // --------------------------------------------------------
    // FIND START BLOCK
    // --------------------------------------------------------

    let start_block =
        find_block_by_timestamp(
            &client,
            &rpc_list,
            start_ts,
            0,
            latest_block
        )
        .await?;


    // --------------------------------------------------------
    // FIND END BLOCK
    // --------------------------------------------------------

    let end_block =
        find_block_by_timestamp(
            &client,
            &rpc_list,
            end_ts.saturating_add(1),
            start_block,
            latest_block
        )
        .await?;


    let end_block =
        end_block.saturating_sub(1);


    println!();
    println!("=======================================================");
    println!("DATE → BLOCK MAPPING");
    println!("=======================================================");
    println!("Start Block : {}", start_block);
    println!("End Block   : {}", end_block);
    println!("Blocks      : {}", end_block.saturating_sub(start_block) + 1);
    println!("=======================================================");


    if start_block > end_block {

        println!(
            "No blocks found in requested date range."
        );

        return Ok(());
    }


    // --------------------------------------------------------
    // RESUME
    // --------------------------------------------------------

    let mut current_block =
        match load_checkpoint() {

            Some(saved) => {

                if saved >= start_block
                    && saved <= end_block + 1
                {
                    println!(
                        "Resuming from checkpoint block {}",
                        saved
                    );

                    saved
                } else {

                    start_block
                }
            }

            None => start_block,
        };


    // --------------------------------------------------------
    // PART NUMBER
    // --------------------------------------------------------

    let mut part_num: u32 = 1;


    // If previous files exist, determine next part.
    while Path::new(
        &format!(
            "output/{}_addresses_1M_part_{:03}.csv.gz",
            chain,
            part_num
        )
    )
    .exists()
    {
        part_num += 1;
    }


    // --------------------------------------------------------
    // MAIN EXTRACTION
    // --------------------------------------------------------

    while current_block <= end_block {

        let chunk_end =
            current_block
                .saturating_add(BLOCK_CHUNK - 1)
                .min(end_block);


        println!();
        println!("=======================================================");
        println!(
            "[PART {:03}] BLOCKS {} → {}",
            part_num,
            current_block,
            chunk_end
        );
        println!("=======================================================");


        // ----------------------------------------------------
        // CREATE 25 BLOCK MICRO CHUNKS
        // ----------------------------------------------------

        let mut micro_chunks:
            Vec<Vec<u64>> = Vec::new();

        let mut b =
            current_block;

        while b <= chunk_end {

            let micro_end =
                b.saturating_add(RPC_BATCH_SIZE - 1)
                    .min(chunk_end);

            micro_chunks.push(
                (b..=micro_end)
                    .collect()
            );

            if micro_end == chunk_end {
                break;
            }

            b = micro_end + 1;
        }


        println!(
            "Micro batches: {}",
            micro_chunks.len()
        );


        // ----------------------------------------------------
        // UNIQUE ADDRESS SET
        // ----------------------------------------------------

        let mut unique_set:
            HashSet<String> =
            HashSet::new();


        // ----------------------------------------------------
        // CONCURRENT RPC
        // ----------------------------------------------------

        let mut stream =
            stream::iter(
                micro_chunks
            )
            .map(|chunk| {

                let client =
                    Arc::clone(&client);

                let rpcs =
                    Arc::clone(&rpc_list);

                tokio::spawn(
                    async move {

                        fetch_rpc_batch_blocks(
                            &client,
                            &rpcs,
                            chunk
                        )
                        .await
                    }
                )
            })
            .buffer_unordered(
                CONCURRENCY
            );


        let mut processed_batches:
            usize = 0;

        let total_batches =
            stream.size_hint().1.unwrap_or(0);


        while let Some(result) =
            stream.next().await
        {

            let addresses =
                result
                    .map_err(|e| {
                        format!(
                            "Worker task failed: {}",
                            e
                        )
                    })?
                    .map_err(|e| {
                        format!(
                            "RPC batch permanently failed: {}",
                            e
                        )
                    })?;


            for address in addresses {

                unique_set.insert(
                    address
                );
            }


            processed_batches += 1;


            if processed_batches % 1000 == 0 {

                println!(
                    "Progress: {}/{} batches | Unique addresses: {}",
                    processed_batches,
                    total_batches,
                    unique_set.len()
                );
            }
        }


        // ----------------------------------------------------
        // WRITE PART
        // ----------------------------------------------------

        if !unique_set.is_empty() {

            let file_name =
                write_addresses_file(
                    &chain,
                    part_num,
                    &unique_set
                )?;

            println!();
            println!(
                "PART {} COMPLETE",
                part_num
            );

            println!(
                "Unique addresses: {}",
                unique_set.len()
            );

            println!(
                "File: {}",
                file_name
            );
        } else {

            println!(
                "Part {} contains zero addresses.",
                part_num
            );
        }


        // ----------------------------------------------------
        // CHECKPOINT
        // ----------------------------------------------------

        let next_block =
            chunk_end.saturating_add(1);

        save_checkpoint(
            next_block
        )?;


        println!(
            "Checkpoint saved: {}",
            next_block
        );


        // ----------------------------------------------------
        // NEXT PART
        // ----------------------------------------------------

        if next_block > end_block {
            break;
        }

        current_block =
            next_block;

        part_num += 1;
    }


    // --------------------------------------------------------
    // CLEAN CHECKPOINT
    // --------------------------------------------------------

    if Path::new(checkpoint_path()).exists() {

        fs::remove_file(
            checkpoint_path()
        )?;
    }


    // --------------------------------------------------------
    // FINAL SUMMARY
    // --------------------------------------------------------

    println!();
    println!("=======================================================");
    println!("EXTRACTION COMPLETED");
    println!("=======================================================");
    println!("Chain       : {}", chain);
    println!("Start Block : {}", start_block);
    println!("End Block   : {}", end_block);
    println!("Output      : ./output/");
    println!("=======================================================");


    Ok(())
}
