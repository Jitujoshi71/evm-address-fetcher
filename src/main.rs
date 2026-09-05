use flate2::write::GzEncoder;
use flate2::Compression;

use futures::stream::{self, StreamExt};

use serde_json::{json, Value};

use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;


// ============================================================
// CONFIGURATION
// ============================================================

const BLOCKS_PER_PART: u64 = 1_000_000;
const BLOCKS_PER_RPC_BATCH: u64 = 25;

const CONCURRENCY: usize = 12;

const MAX_RPC_RETRIES: usize = 3;

const RPC_TIMEOUT_SECONDS: u64 = 45;


// ============================================================
// RPC LIST
// ============================================================

fn get_rpc_list(chain: &str) -> Vec<String> {
    let mut list = Vec::new();

    // User supplied RPC gets priority.
    if let Ok(custom_rpc) = env::var("RPC_URL") {
        let rpc = custom_rpc.trim();

        if !rpc.is_empty() {
            list.push(rpc.to_string());
        }
    }

    let defaults: Vec<String> = match chain {
        "bnb" => vec![
            "https://bsc-dataseed.binance.org/".to_string(),
            "https://bsc-dataseed1.defibit.io/".to_string(),
            "https://bsc-dataseed1.ninicoin.io/".to_string(),
            "https://rpc.ankr.com/bsc".to_string(),
            "https://1rpc.io/bnb".to_string(),
        ],

        "ethereum" => vec![
            "https://eth.llamarpc.com".to_string(),
            "https://cloudflare-eth.com".to_string(),
            "https://rpc.ankr.com/eth".to_string(),
            "https://1rpc.io/eth".to_string(),
        ],

        "polygon" => vec![
            "https://polygon-rpc.com".to_string(),
            "https://rpc.ankr.com/polygon".to_string(),
            "https://1rpc.io/matic".to_string(),
        ],

        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc".to_string(),
            "https://rpc.ankr.com/arbitrum".to_string(),
            "https://1rpc.io/arb".to_string(),
        ],

        "base" => vec![
            "https://mainnet.base.org".to_string(),
            "https://base.llamarpc.com".to_string(),
            "https://1rpc.io/base".to_string(),
        ],

        "optimism" => vec![
            "https://mainnet.optimism.io".to_string(),
            "https://optimism.llamarpc.com".to_string(),
            "https://1rpc.io/op".to_string(),
        ],

        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc".to_string(),
            "https://rpc.ankr.com/avalanche".to_string(),
            "https://1rpc.io/avax/c".to_string(),
        ],

        _ => {
            return Err(format!("Unsupported chain: {}", chain))
                .unwrap_or_else(|_| vec![]);
        }
    };

    for rpc in defaults {
        if !list.contains(&rpc) {
            list.push(rpc);
        }
    }

    list
}


// ============================================================
// HEX TO U64
// ============================================================

fn hex_to_u64(value: &str) -> Option<u64> {
    let clean = value.trim_start_matches("0x");

    u64::from_str_radix(clean, 16).ok()
}


// ============================================================
// GENERIC RPC CALL
// ============================================================

async fn rpc_call(
    client: &reqwest::Client,
    rpc_urls: &[String],
    payload: Value,
) -> Result<Value, String> {
    let mut last_error = String::from("Unknown RPC error");

    for rpc in rpc_urls {
        for attempt in 1..=MAX_RPC_RETRIES {
            match client
                .post(rpc)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();

                    if !status.is_success() {
                        last_error = format!(
                            "{} returned HTTP {}",
                            rpc,
                            status
                        );
                    } else {
                        match response.json::<Value>().await {
                            Ok(value) => {
                                if value.get("error").is_some() {
                                    last_error = format!(
                                        "{} returned RPC error: {}",
                                        rpc,
                                        value["error"]
                                    );
                                } else {
                                    return Ok(value);
                                }
                            }

                            Err(error) => {
                                last_error = format!(
                                    "{} returned invalid JSON: {}",
                                    rpc,
                                    error
                                );
                            }
                        }
                    }
                }

                Err(error) => {
                    last_error = format!(
                        "{} request failed: {}",
                        rpc,
                        error
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
            "RPC failed, switching to next RPC: {}",
            rpc
        );
    }

    Err(last_error)
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
        .ok_or_else(|| {
            "Missing eth_blockNumber result".to_string()
        })?;

    hex_to_u64(result)
        .ok_or_else(|| {
            "Invalid latest block number".to_string()
        })
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
        "id": block
    });

    let response = rpc_call(
        client,
        rpc_urls,
        payload
    )
    .await?;

    let result = response
        .get("result")
        .ok_or_else(|| {
            format!("Block {} has no result", block)
        })?;

    let timestamp_hex = result
        .get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            format!(
                "Block {} has no timestamp",
                block
            )
        })?;

    hex_to_u64(timestamp_hex)
        .ok_or_else(|| {
            format!(
                "Invalid timestamp in block {}",
                block
            )
        })
}


// ============================================================
// FIND FIRST BLOCK WITH TIMESTAMP >= TARGET
// ============================================================

async fn find_first_block_at_or_after(
    client: &reqwest::Client,
    rpc_urls: &[String],
    target_timestamp: u64,
    mut low: u64,
    mut high: u64,
) -> Result<u64, String> {
    while low < high {
        let mid = low + (high - low) / 2;

        let timestamp = get_block_timestamp(
            client,
            rpc_urls,
            mid
        )
        .await?;

        if timestamp < target_timestamp {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    Ok(low)
}


// ============================================================
// FETCH 25 BLOCKS
// ============================================================

async fn fetch_rpc_batch_blocks(
    client: &reqwest::Client,
    rpc_urls: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, String> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }

    let payload: Vec<Value> = blocks
        .iter()
        .map(|block| {
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

    let mut last_error =
        String::from("Batch request failed");

    for rpc in rpc_urls {
        for attempt in 1..=MAX_RPC_RETRIES {
            match client
                .post(rpc)
                .json(&payload)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        last_error = format!(
                            "{} HTTP {}",
                            rpc,
                            response.status()
                        );
                    } else {
                        match response
                            .json::<Vec<Value>>()
                            .await
                        {
                            Ok(results) => {
                                let mut addresses =
                                    Vec::new();

                                let mut received =
                                    HashSet::<u64>::new();

                                for item in results {
                                    if item.get("error").is_some() {
                                        last_error =
                                            format!(
                                                "{} RPC error: {}",
                                                rpc,
                                                item["error"]
                                            );

                                        continue;
                                    }

                                    let result =
                                        match item.get("result") {
                                            Some(value)
                                                if !value.is_null() =>
                                            {
                                                value
                                            }

                                            _ => {
                                                continue;
                                            }
                                        };

                                    if let Some(number) =
                                        result
                                            .get("number")
                                            .and_then(|v| v.as_str())
                                            .and_then(hex_to_u64)
                                    {
                                        received.insert(number);
                                    }

                                    if let Some(transactions) =
                                        result
                                            .get("transactions")
                                            .and_then(|v| v.as_array())
                                    {
                                        for tx in transactions {
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

                                // Never silently accept partial batch.
                                if received.len() == blocks.len() {
                                    return Ok(addresses);
                                }

                                last_error = format!(
                                    "{} returned {}/{} blocks",
                                    rpc,
                                    received.len(),
                                    blocks.len()
                                );
                            }

                            Err(error) => {
                                last_error = format!(
                                    "{} invalid JSON: {}",
                                    rpc,
                                    error
                                );
                            }
                        }
                    }
                }

                Err(error) => {
                    last_error = format!(
                        "{} request failed: {}",
                        rpc,
                        error
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
            "Batch failed on RPC: {}",
            rpc
        );
    }

    Err(last_error)
}


// ============================================================
// CHECKPOINT
// ============================================================

fn checkpoint_file() -> &'static str {
    "output/checkpoint.txt"
}


fn save_checkpoint(
    block: u64
) -> Result<(), Box<dyn std::error::Error>> {
    let temp =
        "output/checkpoint.tmp";

    fs::write(
        temp,
        block.to_string()
    )?;

    fs::rename(
        temp,
        checkpoint_file()
    )?;

    Ok(())
}


fn load_checkpoint() -> Option<u64> {
    if !Path::new(checkpoint_file()).exists() {
        return None;
    }

    let content =
        fs::read_to_string(checkpoint_file())
            .ok()?;

    content.trim().parse::<u64>().ok()
}


// ============================================================
// FIND NEXT PART NUMBER
// ============================================================

fn next_part_number(chain: &str) -> u32 {
    let mut part = 1u32;

    loop {
        let path = format!(
            "output/{}_addresses_1M_part_{:03}.csv.gz",
            chain,
            part
        );

        if !Path::new(&path).exists() {
            return part;
        }

        part += 1;
    }
}


// ============================================================
// WRITE CSV.GZ
// ============================================================

fn write_addresses_file(
    chain: &str,
    part: u32,
    addresses: &HashSet<String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let file_name = format!(
        "output/{}_addresses_1M_part_{:03}.csv.gz",
        chain,
        part
    );

    let file =
        File::create(&file_name)?;

    let encoder =
        GzEncoder::new(
            file,
            Compression::default()
        );

    let mut writer =
        csv::Writer::from_writer(
            encoder
        );

    writer.write_record(["address"])?;

    for address in addresses {
        writer.write_record([address])?;
    }

    writer.flush()?;

    let encoder =
        writer
            .into_inner()
            .map_err(|error| error.into_error())?;

    encoder.finish()?;

    Ok(file_name)
}


// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error>>
{
    // ========================================================
    // ARGUMENTS
    // ========================================================

    let args: Vec<String> =
        env::args().collect();

    let mut chain =
        env::var("TARGET_CHAIN")
            .unwrap_or_else(
                |_| "bnb".to_string()
            );

    let mut start_ts =
        env::var("START_TIMESTAMP")
            .ok()
            .and_then(
                |v| v.parse::<u64>().ok()
            );

    let mut end_ts =
        env::var("END_TIMESTAMP")
            .ok()
            .and_then(
                |v| v.parse::<u64>().ok()
            );


    let mut index = 1usize;

    while index < args.len() {
        match args[index].as_str() {
            "--chain" => {
                index += 1;

                if index < args.len() {
                    chain =
                        args[index].clone();
                }
            }

            "--start-ts" => {
                index += 1;

                if index < args.len() {
                    start_ts = Some(
                        args[index]
                            .parse::<u64>()?
                    );
                }
            }

            "--end-ts" => {
                index += 1;

                if index < args.len() {
                    end_ts = Some(
                        args[index]
                            .parse::<u64>()?
                    );
                }
            }

            _ => {}
        }

        index += 1;
    }


    let start_ts =
        start_ts.ok_or(
            "Missing --start-ts / START_TIMESTAMP"
        )?;

    let end_ts =
        end_ts.ok_or(
            "Missing --end-ts / END_TIMESTAMP"
        )?;


    if start_ts > end_ts {
        return Err(
            "Start timestamp cannot be greater than end timestamp"
                .into()
        );
    }


    // ========================================================
    // OUTPUT DIRECTORY
    // ========================================================

    fs::create_dir_all("output")?;


    // ========================================================
    // RPC
    // ========================================================

    let rpc_list =
        get_rpc_list(&chain);

    if rpc_list.is_empty() {
        return Err(
            "No RPC endpoints configured".into()
        );
    }


    println!();
    println!("=======================================================");
    println!("EVM ADDRESS EXTRACTOR");
    println!("=======================================================");
    println!("Chain       : {}", chain);
    println!("Start TS    : {}", start_ts);
    println!("End TS      : {}", end_ts);
    println!("RPC count   : {}", rpc_list.len());
    println!("Blocks/part : {}", BLOCKS_PER_PART);
    println!("RPC batch   : {}", BLOCKS_PER_RPC_BATCH);
    println!("Concurrency : {}", CONCURRENCY);
    println!("=======================================================");


    for (i, rpc) in rpc_list.iter().enumerate() {
        println!(
            "RPC #{}: {}",
            i + 1,
            rpc
        );
    }


    // ========================================================
    // HTTP CLIENT
    // ========================================================

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


    // ========================================================
    // LATEST BLOCK
    // ========================================================

    let latest_block =
        get_latest_block(
            &client,
            &rpc_list
        )
        .await?;

    println!();
    println!(
        "Latest block: {}",
        latest_block
    );


    // ========================================================
    // DATE → START BLOCK
    // ========================================================

    println!();
    println!(
        "Finding START block for timestamp {}...",
        start_ts
    );

    let start_block =
        find_first_block_at_or_after(
            &client,
            &rpc_list,
            start_ts,
            0,
            latest_block
        )
        .await?;


    // ========================================================
    // DATE → END BLOCK
    //
    // We search for END_TIMESTAMP + 1.
    // Therefore all blocks with timestamp <= END_TIMESTAMP
    // are included.
    // ========================================================

    println!();
    println!(
        "Finding END block for timestamp {}...",
        end_ts
    );

    let end_search_timestamp =
        end_ts.saturating_add(1);

    let first_after_end =
        find_first_block_at_or_after(
            &client,
            &rpc_list,
            end_search_timestamp,
            start_block,
            latest_block
        )
        .await?;


    let end_block =
        if first_after_end > 0 {
            first_after_end - 1
        } else {
            0
        };


    // ========================================================
    // RANGE CHECK
    // ========================================================

    println!();
    println!("=======================================================");
    println!("DATE → BLOCK RESULT");
    println!("=======================================================");
    println!("Start Block : {}", start_block);
    println!("End Block   : {}", end_block);

    if start_block <= end_block {
        println!(
            "Total Blocks: {}",
            end_block - start_block + 1
        );
    } else {
        println!("Total Blocks: 0");
    }

    println!("=======================================================");


    if start_block > end_block {
        println!(
            "No blocks exist in the requested date range."
        );

        return Ok(());
    }


    // ========================================================
    // RESUME CHECKPOINT
    // ========================================================

    let mut current_block =
        match load_checkpoint() {
            Some(saved)
                if saved >= start_block
                    && saved <= end_block + 1 =>
            {
                println!(
                    "Resuming from checkpoint: {}",
                    saved
                );

                saved
            }

            _ => {
                println!(
                    "Starting extraction from block {}",
                    start_block
                );

                start_block
            }
        };


    // ========================================================
    // PART NUMBER
    // ========================================================

    let mut part_num =
        next_part_number(&chain);


    // ========================================================
    // MAIN EXTRACTION LOOP
    // ========================================================

    while current_block <= end_block {
        let chunk_end =
            current_block
                .saturating_add(
                    BLOCKS_PER_PART - 1
                )
                .min(end_block);


        println!();
        println!("=======================================================");
        println!(
            "[PART {:03}] {} → {}",
            part_num,
            current_block,
            chunk_end
        );
        println!("=======================================================");


        // ----------------------------------------------------
        // CREATE MICRO BATCHES
        // ----------------------------------------------------

        let mut micro_chunks:
            Vec<Vec<u64>> =
            Vec::new();

        let mut block =
            current_block;

        while block <= chunk_end {
            let micro_end =
                block
                    .saturating_add(
                        BLOCKS_PER_RPC_BATCH - 1
                    )
                    .min(chunk_end);

            micro_chunks.push(
                (block..=micro_end)
                    .collect()
            );

            if micro_end == chunk_end {
                break;
            }

            block =
                micro_end + 1;
        }


        let total_batches =
            micro_chunks.len();


        println!(
            "Micro batches: {}",
            total_batches
        );


        // ----------------------------------------------------
        // UNIQUE ADDRESSES
        // ----------------------------------------------------

        let mut unique_set:
            HashSet<String> =
            HashSet::new();


        // ----------------------------------------------------
        // CONCURRENT RPC STREAM
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


        // ----------------------------------------------------
        // PROCESS RESULTS
        // ----------------------------------------------------

        let mut processed_batches =
            0usize;

        while let Some(result) =
            stream.next().await
        {
            let addresses =
                result
                    .map_err(|error| {
                        format!(
                            "Worker task failed: {}",
                            error
                        )
                    })?
                    .map_err(|error| {
                        format!(
                            "RPC batch failed permanently: {}",
                            error
                        )
                    })?;


            for address in addresses {
                unique_set.insert(
                    address
                );
            }


            processed_batches += 1;


            if processed_batches % 1000 == 0
                || processed_batches == total_batches
            {
                let percentage =
                    (
                        processed_batches as f64
                        /
                        total_batches as f64
                    ) * 100.0;

                println!(
                    "Progress: {}/{} ({:.2}%) | Unique addresses: {}",
                    processed_batches,
                    total_batches,
                    percentage,
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
                "PART {:03} COMPLETE",
                part_num
            );

            println!(
                "Unique addresses: {}",
                unique_set.len()
            );

            println!(
                "Output file: {}",
                file_name
            );
        } else {
            println!(
                "PART {:03}: no addresses found.",
                part_num
            );
        }


        // ----------------------------------------------------
        // SAVE CHECKPOINT
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


    // ========================================================
    // REMOVE CHECKPOINT AFTER COMPLETE SUCCESS
    // ========================================================

    if Path::new(
        checkpoint_file()
    ).exists()
    {
        fs::remove_file(
            checkpoint_file()
        )?;
    }


    // ========================================================
    // FINAL SUMMARY
    // ========================================================

    println!();
    println!("=======================================================");
    println!("EXTRACTION COMPLETED SUCCESSFULLY");
    println!("=======================================================");
    println!("Chain       : {}", chain);
    println!("Start Block : {}", start_block);
    println!("End Block   : {}", end_block);
    println!("Output      : ./output/");
    println!("=======================================================");


    Ok(())
}
