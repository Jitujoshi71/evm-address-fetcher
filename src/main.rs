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
// CONFIG
// ============================================================

const BLOCKS_PER_PART: u64 = 1_000_000;

// Small RPC batches because eth_getBlockByNumber(true)
// can return very large responses.
const BLOCKS_PER_RPC_BATCH: u64 = 5;

// Number of simultaneous RPC jobs.
const CONCURRENCY: usize = 6;

// Retries on each RPC.
const MAX_RPC_RETRIES: usize = 2;

const RPC_TIMEOUT_SECONDS: u64 = 45;

// ============================================================
// RPC LIST
// ============================================================

fn get_rpc_list(chain: &str) -> Result<Vec<String>, String> {
    let mut rpc_list = Vec::new();

    // User supplied RPC is tried first if available.
    if let Ok(custom_rpc) = env::var("RPC_URL") {
        let custom_rpc = custom_rpc.trim();

        if !custom_rpc.is_empty() {
            rpc_list.push(custom_rpc.to_string());
        }
    }

    let defaults = match chain {
        "bnb" => vec![
            "https://bsc-dataseed.binance.org/",
            "https://bsc-dataseed1.defibit.io/",
            "https://bsc-dataseed1.ninicoin.io/",
            "https://bsc-dataseed2.binance.org/",
            "https://bsc-dataseed3.binance.org/",
            "https://rpc.ankr.com/bsc",
            "https://1rpc.io/bnb",
        ],

        "ethereum" => vec![
            "https://eth.llamarpc.com",
            "https://cloudflare-eth.com",
            "https://rpc.ankr.com/eth",
            "https://1rpc.io/eth",
        ],

        "polygon" => vec![
            "https://polygon-rpc.com",
            "https://rpc.ankr.com/polygon",
            "https://1rpc.io/matic",
        ],

        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc",
            "https://rpc.ankr.com/arbitrum",
            "https://1rpc.io/arb",
        ],

        "base" => vec![
            "https://mainnet.base.org",
            "https://base.llamarpc.com",
            "https://1rpc.io/base",
        ],

        "optimism" => vec![
            "https://mainnet.optimism.io",
            "https://optimism.llamarpc.com",
            "https://1rpc.io/op",
        ],

        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc",
            "https://rpc.ankr.com/avalanche",
            "https://1rpc.io/avax/c",
        ],

        _ => {
            return Err(format!("Unsupported chain: {}", chain));
        }
    };

    for rpc in defaults {
        if !rpc_list.iter().any(|x| x == rpc) {
            rpc_list.push(rpc.to_string());
        }
    }

    Ok(rpc_list)
}

// ============================================================
// HEX
// ============================================================

fn hex_to_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

// ============================================================
// RPC SINGLE REQUEST
// ============================================================

async fn rpc_single_call(
    client: &reqwest::Client,
    rpc: &str,
    payload: &Value,
) -> Result<Value, String> {
    let response = client
        .post(rpc)
        .json(payload)
        .send()
        .await
        .map_err(|e| format!("request error: {}", e))?;

    let status = response.status();

    if !status.is_success() {
        return Err(format!("HTTP {}", status));
    }

    let body = response
        .text()
        .await
        .map_err(|e| format!("response read error: {}", e))?;

    if body.trim().is_empty() {
        return Err("empty response".to_string());
    }

    serde_json::from_str::<Value>(&body)
        .map_err(|e| format!("invalid JSON: {}", e))
}

// ============================================================
// GENERIC RPC FAILOVER
// ============================================================

async fn rpc_call_failover(
    client: &reqwest::Client,
    rpc_urls: &[String],
    payload: Value,
) -> Result<Value, String> {
    let mut last_error = String::from("unknown RPC error");

    for (rpc_index, rpc) in rpc_urls.iter().enumerate() {
        for attempt in 1..=MAX_RPC_RETRIES {
            match rpc_single_call(client, rpc, &payload).await {
                Ok(value) => {
                    if value.get("error").is_some() {
                        last_error = format!(
                            "RPC error from {}: {}",
                            rpc,
                            value["error"]
                        );
                    } else {
                        if rpc_index > 0 || attempt > 1 {
                            println!(
                                "RPC recovered using {} (attempt {})",
                                rpc,
                                attempt
                            );
                        }

                        return Ok(value);
                    }
                }

                Err(error) => {
                    last_error =
                        format!("{} -> {}", rpc, error);
                }
            }

            if attempt < MAX_RPC_RETRIES {
                sleep(Duration::from_millis(
                    300 * attempt as u64,
                ))
                .await;
            }
        }

        eprintln!(
            "RPC failed, switching to next RPC: {} | {}",
            rpc,
            last_error
        );
    }

    Err(last_error)
}

// ============================================================
// LATEST BLOCK
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

    let response =
        rpc_call_failover(client, rpc_urls, payload).await?;

    let result = response
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            "Missing eth_blockNumber result".to_string()
        })?;

    hex_to_u64(result)
        .ok_or_else(|| "Invalid block number".to_string())
}

// ============================================================
// BLOCK TIMESTAMP
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

    let response =
        rpc_call_failover(client, rpc_urls, payload).await?;

    let result = response
        .get("result")
        .ok_or_else(|| {
            format!("Block {} missing result", block)
        })?;

    if result.is_null() {
        return Err(format!(
            "Block {} returned null",
            block
        ));
    }

    let timestamp = result
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(hex_to_u64)
        .ok_or_else(|| {
            format!(
                "Block {} has invalid timestamp",
                block
            )
        })?;

    Ok(timestamp)
}

// ============================================================
// FIND FIRST BLOCK AT / AFTER TIMESTAMP
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

        let timestamp =
            get_block_timestamp(
                client,
                rpc_urls,
                mid,
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
// PARSE BATCH RESULT
// ============================================================

fn parse_batch_result(
    results: Vec<Value>,
    requested_blocks: &[u64],
) -> Result<Vec<String>, String> {
    let mut addresses = Vec::new();

    let mut received_blocks =
        HashSet::<u64>::new();

    for item in results {
        if item.get("error").is_some() {
            continue;
        }

        let result = match item.get("result") {
            Some(v) if !v.is_null() => v,
            _ => continue,
        };

        if let Some(number) = result
            .get("number")
            .and_then(|v| v.as_str())
            .and_then(hex_to_u64)
        {
            received_blocks.insert(number);
        }

        if let Some(transactions) = result
            .get("transactions")
            .and_then(|v| v.as_array())
        {
            for tx in transactions {
                if let Some(from) =
                    tx.get("from").and_then(|v| v.as_str())
                {
                    addresses.push(
                        from.to_ascii_lowercase()
                    );
                }

                if let Some(to) =
                    tx.get("to").and_then(|v| v.as_str())
                {
                    addresses.push(
                        to.to_ascii_lowercase()
                    );
                }
            }
        }
    }

    if received_blocks.len() != requested_blocks.len() {
        return Err(format!(
            "Incomplete batch: received {}/{} blocks",
            received_blocks.len(),
            requested_blocks.len()
        ));
    }

    Ok(addresses)
}

// ============================================================
// FETCH BLOCK BATCH WITH ALL RPC FAILOVER
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
        String::from("batch failed");

    for (rpc_index, rpc) in rpc_urls.iter().enumerate() {
        for attempt in 1..=MAX_RPC_RETRIES {
            let response = client
                .post(rpc)
                .json(&payload)
                .send()
                .await;

            match response {
                Ok(response) => {
                    if !response.status().is_success() {
                        last_error = format!(
                            "{} HTTP {}",
                            rpc,
                            response.status()
                        );
                    } else {
                        match response.text().await {
                            Ok(body) => {
                                if body.trim().is_empty() {
                                    last_error = format!(
                                        "{} empty response",
                                        rpc
                                    );
                                } else {
                                    match serde_json::from_str::<Vec<Value>>(
                                        &body
                                    ) {
                                        Ok(results) => {
                                            match parse_batch_result(
                                                results,
                                                &blocks,
                                            ) {
                                                Ok(addresses) => {
                                                    if rpc_index > 0
                                                        || attempt > 1
                                                    {
                                                        println!(
                                                            "Batch recovered: RPC #{} {}",
                                                            rpc_index + 1,
                                                            rpc
                                                        );
                                                    }

                                                    return Ok(addresses);
                                                }

                                                Err(error) => {
                                                    last_error =
                                                        format!(
                                                            "{}: {}",
                                                            rpc,
                                                            error
                                                        );
                                                }
                                            }
                                        }

                                        Err(error) => {
                                            last_error =
                                                format!(
                                                    "{} invalid JSON: {}",
                                                    rpc,
                                                    error
                                                );
                                        }
                                    }
                                }
                            }

                            Err(error) => {
                                last_error =
                                    format!(
                                        "{} response read error: {}",
                                        rpc,
                                        error
                                    );
                            }
                        }
                    }
                }

                Err(error) => {
                    last_error =
                        format!(
                            "{} request error: {}",
                            rpc,
                            error
                        );
                }
            }

            if attempt < MAX_RPC_RETRIES {
                sleep(Duration::from_millis(
                    300 * attempt as u64,
                ))
                .await;
            }
        }

        eprintln!(
            "Batch RPC failed → switching RPC | {}",
            last_error
        );
    }

    Err(last_error)
}

// ============================================================
// RESILIENT FETCH
//
// First tries complete batch.
//
// If ALL RPCs fail:
// 5 blocks
//   ↓
// individual blocks
//   ↓
// every RPC gets tried again
//
// This prevents a single bad RPC from killing the run.
// ============================================================

async fn fetch_resilient(
    client: &reqwest::Client,
    rpc_urls: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, String> {
    match fetch_rpc_batch_blocks(
        client,
        rpc_urls,
        blocks.clone(),
    )
    .await
    {
        Ok(addresses) => Ok(addresses),

        Err(batch_error) => {
            eprintln!(
                "Complete batch failed: {}",
                batch_error
            );

            eprintln!(
                "Falling back to individual block RPC requests..."
            );

            let mut all_addresses =
                Vec::new();

            for block in blocks {
                let mut success = false;
                let mut last_error =
                    String::new();

                for (rpc_index, rpc) in
                    rpc_urls.iter().enumerate()
                {
                    for attempt in 1..=MAX_RPC_RETRIES {
                        let payload = json!({
                            "jsonrpc": "2.0",
                            "method": "eth_getBlockByNumber",
                            "params": [
                                format!("0x{:x}", block),
                                true
                            ],
                            "id": block
                        });

                        match rpc_single_call(
                            client,
                            rpc,
                            &payload,
                        )
                        .await
                        {
                            Ok(response) => {
                                if let Some(result) =
                                    response.get("result")
                                {
                                    if !result.is_null() {
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
                                                    all_addresses.push(
                                                        from.to_ascii_lowercase()
                                                    );
                                                }

                                                if let Some(to) =
                                                    tx.get("to")
                                                        .and_then(|v| v.as_str())
                                                {
                                                    all_addresses.push(
                                                        to.to_ascii_lowercase()
                                                    );
                                                }
                                            }
                                        }

                                        if rpc_index > 0 {
                                            println!(
                                                "Block {} recovered using RPC #{}",
                                                block,
                                                rpc_index + 1
                                            );
                                        }

                                        success = true;
                                        break;
                                    }
                                }

                                last_error =
                                    format!(
                                        "{} returned invalid block result",
                                        rpc
                                    );
                            }

                            Err(error) => {
                                last_error =
                                    format!(
                                        "{}: {}",
                                        rpc,
                                        error
                                    );
                            }
                        }

                        if success {
                            break;
                        }

                        if attempt < MAX_RPC_RETRIES {
                            sleep(
                                Duration::from_millis(
                                    300 * attempt as u64
                                )
                            )
                            .await;
                        }
                    }

                    if success {
                        break;
                    }

                    eprintln!(
                        "Block {} failed on RPC #{}: {}",
                        block,
                        rpc_index + 1,
                        last_error
                    );
                }

                if !success {
                    return Err(format!(
                        "Block {} failed on ALL RPCs: {}",
                        block,
                        last_error
                    ));
                }
            }

            Ok(all_addresses)
        }
    }
}

// ============================================================
// CHECKPOINT
// ============================================================

fn checkpoint_file() -> &'static str {
    "output/checkpoint.txt"
}

fn save_checkpoint(
    block: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp =
        "output/checkpoint.tmp";

    fs::write(
        tmp,
        block.to_string()
    )?;

    fs::rename(
        tmp,
        checkpoint_file()
    )?;

    Ok(())
}

fn load_checkpoint() -> Option<u64> {
    if !Path::new(
        checkpoint_file()
    )
    .exists()
    {
        return None;
    }

    fs::read_to_string(
        checkpoint_file()
    )
    .ok()?
    .trim()
    .parse::<u64>()
    .ok()
}

// ============================================================
// NEXT PART
// ============================================================

fn next_part_number(
    chain: &str,
) -> u32 {
    let mut part = 1u32;

    loop {
        let path =
            format!(
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
    let file_name =
        format!(
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

    writer.write_record(
        ["address"]
    )?;

    for address in addresses {
        writer.write_record(
            [address]
        )?;
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
    // ENV
    // ========================================================

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

    // ========================================================
    // CLI
    // ========================================================

    let args:
        Vec<String> =
        env::args().collect();

    let mut i = 1usize;

    while i < args.len() {
        match args[i].as_str() {
            "--chain" => {
                i += 1;

                if i < args.len() {
                    chain =
                        args[i].clone();
                }
            }

            "--start-ts" => {
                i += 1;

                if i < args.len() {
                    start_ts =
                        Some(
                            args[i]
                                .parse::<u64>()?
                        );
                }
            }

            "--end-ts" => {
                i += 1;

                if i < args.len() {
                    end_ts =
                        Some(
                            args[i]
                                .parse::<u64>()?
                        );
                }
            }

            _ => {}
        }

        i += 1;
    }

    let start_ts =
        start_ts.ok_or(
            "Missing START_TIMESTAMP"
        )?;

    let end_ts =
        end_ts.ok_or(
            "Missing END_TIMESTAMP"
        )?;

    if start_ts > end_ts {
        return Err(
            "Start timestamp cannot be greater than end timestamp"
                .into()
        );
    }

    // ========================================================
    // OUTPUT
    // ========================================================

    fs::create_dir_all(
        "output"
    )?;

    // ========================================================
    // RPCs
    // ========================================================

    let rpc_list =
        get_rpc_list(
            &chain
        )?;

    if rpc_list.is_empty() {
        return Err(
            "No RPC endpoints available"
                .into()
        );
    }

    // ========================================================
    // INFO
    // ========================================================

    println!();
    println!(
        "======================================================="
    );
    println!(
        "EVM ADDRESS EXTRACTOR"
    );
    println!(
        "======================================================="
    );
    println!(
        "Chain              : {}",
        chain
    );
    println!(
        "Start Timestamp    : {}",
        start_ts
    );
    println!(
        "End Timestamp      : {}",
        end_ts
    );
    println!(
        "Blocks / Part      : {}",
        BLOCKS_PER_PART
    );
    println!(
        "Blocks / RPC Batch : {}",
        BLOCKS_PER_RPC_BATCH
    );
    println!(
        "Concurrency        : {}",
        CONCURRENCY
    );
    println!(
        "RPC Count          : {}",
        rpc_list.len()
    );
    println!(
        "======================================================="
    );

    for (
        index,
        rpc,
    ) in rpc_list.iter().enumerate()
    {
        println!(
            "RPC #{} : {}",
            index + 1,
            rpc
        );
    }

    println!(
        "======================================================="
    );

    // ========================================================
    // HTTP CLIENT
    // ========================================================

    let client =
        reqwest::Client::builder()
            .timeout(
                Duration::from_secs(
                    RPC_TIMEOUT_SECONDS
                )
            )
            .pool_max_idle_per_host(
                32
            )
            .build()?;

    let client =
        Arc::new(client);

    let rpc_list =
        Arc::new(rpc_list);

    // ========================================================
    // LATEST BLOCK
    // ========================================================

    println!();
    println!(
        "Getting latest block..."
    );

    let latest_block =
        get_latest_block(
            &client,
            &rpc_list,
        )
        .await?;

    println!(
        "Latest Block: {}",
        latest_block
    );

    let latest_timestamp =
        get_block_timestamp(
            &client,
            &rpc_list,
            latest_block,
        )
        .await?;

    println!(
        "Latest Timestamp: {}",
        latest_timestamp
    );

    // ========================================================
    // FUTURE DATE CHECK
    // ========================================================

    if start_ts > latest_timestamp {
        return Err(
            "Requested start date is in the future."
                .into()
        );
    }

    // ========================================================
    // START BLOCK
    // ========================================================

    println!();
    println!(
        "Searching start block..."
    );

    let start_block =
        find_first_block_at_or_after(
            &client,
            &rpc_list,
            start_ts,
            0,
            latest_block,
        )
        .await?;

    let start_block_timestamp =
        get_block_timestamp(
            &client,
            &rpc_list,
            start_block,
        )
        .await?;

    // ========================================================
    // END BLOCK
    // ========================================================

    let end_block;

    if end_ts >= latest_timestamp {
        end_block =
            latest_block;
    } else {
        println!(
            "Searching end block..."
        );

        let first_after_end =
            find_first_block_at_or_after(
                &client,
                &rpc_list,
                end_ts.saturating_add(1),
                start_block,
                latest_block,
            )
            .await?;

        end_block =
            first_after_end.saturating_sub(1);
    }

    let end_block_timestamp =
        get_block_timestamp(
            &client,
            &rpc_list,
            end_block,
        )
        .await?;

    // ========================================================
    // MAPPING
    // ========================================================

    println!();
    println!(
        "======================================================="
    );
    println!(
        "DATE → BLOCK MAPPING"
    );
    println!(
        "======================================================="
    );
    println!(
        "Start Block       : {}",
        start_block
    );
    println!(
        "Start Block Time  : {}",
        start_block_timestamp
    );
    println!(
        "End Block         : {}",
        end_block
    );
    println!(
        "End Block Time    : {}",
        end_block_timestamp
    );
    println!(
        "Total Blocks      : {}",
        end_block
            .saturating_sub(start_block)
            + 1
    );
    println!(
        "======================================================="
    );

    if start_block > end_block {
        println!(
            "No blocks found."
        );

        return Ok(());
    }

    // ========================================================
    // CHECKPOINT
    // ========================================================

    let mut current_block =
        match load_checkpoint() {
            Some(saved)
                if saved >= start_block
                    && saved <= end_block + 1 =>
            {
                println!();
                println!(
                    "Checkpoint found."
                );
                println!(
                    "Resuming from block {}",
                    saved
                );

                saved
            }

            _ => {
                start_block
            }
        };

    // ========================================================
    // PART NUMBER
    // ========================================================

    let mut part_num =
        next_part_number(
            &chain
        );

    // ========================================================
    // EXTRACTION
    // ========================================================

    while current_block <= end_block {
        let chunk_end =
            current_block
                .saturating_add(
                    BLOCKS_PER_PART - 1
                )
                .min(
                    end_block
                );

        println!();
        println!(
            "======================================================="
        );
        println!(
            "[PART {:03}] {} → {}",
            part_num,
            current_block,
            chunk_end
        );
        println!(
            "======================================================="
        );

        // ----------------------------------------------------
        // MICRO CHUNKS
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
                    .min(
                        chunk_end
                    );

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
            "Total RPC batches: {}",
            total_batches
        );

        // ----------------------------------------------------
        // UNIQUE ADDRESSES
        // ----------------------------------------------------

        let mut unique_set:
            HashSet<String> =
            HashSet::new();

        // ----------------------------------------------------
        // CONCURRENT WORKERS
        // ----------------------------------------------------

        let mut stream =
            stream::iter(
                micro_chunks
            )
            .map(
                |chunk| {
                    let client =
                        Arc::clone(
                            &client
                        );

                    let rpcs =
                        Arc::clone(
                            &rpc_list
                        );

                    tokio::spawn(
                        async move {
                            fetch_resilient(
                                &client,
                                &rpcs,
                                chunk,
                            )
                            .await
                        }
                    )
                }
            )
            .buffer_unordered(
                CONCURRENCY
            );

        // ----------------------------------------------------
        // RESULTS
        // ----------------------------------------------------

        let mut processed =
            0usize;

        while let Some(result) =
            stream.next().await
        {
            let addresses =
                result
                    .map_err(
                        |e| {
                            format!(
                                "Worker failed: {}",
                                e
                            )
                        }
                    )?
                    .map_err(
                        |e| {
                            format!(
                                "Permanent block failure: {}",
                                e
                            )
                        }
                    )?;

            for address in addresses {
                unique_set.insert(
                    address
                );
            }

            processed += 1;

            if processed % 500 == 0
                || processed == total_batches
            {
                let percentage =
                    processed as f64
                    /
                    total_batches as f64
                    *
                    100.0;

                println!(
                    "Progress: {}/{} ({:.2}%) | Unique addresses: {}",
                    processed,
                    total_batches,
                    percentage,
                    unique_set.len()
                );
            }
        }

        // ----------------------------------------------------
        // WRITE
        // ----------------------------------------------------

        if !unique_set.is_empty() {
            let file_name =
                write_addresses_file(
                    &chain,
                    part_num,
                    &unique_set,
                )?;

            println!();
            println!(
                "======================================================="
            );
            println!(
                "PART {:03} COMPLETE",
                part_num
            );
            println!(
                "Blocks           : {} → {}",
                current_block,
                chunk_end
            );
            println!(
                "Unique Addresses : {}",
                unique_set.len()
            );
            println!(
                "File             : {}",
                file_name
            );
            println!(
                "======================================================="
            );
        } else {
            println!(
                "PART {:03}: no addresses found.",
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

        if next_block > end_block {
            break;
        }

        current_block =
            next_block;

        part_num += 1;
    }

    // ========================================================
    // REMOVE CHECKPOINT
    // ========================================================

    if Path::new(
        checkpoint_file()
    )
    .exists()
    {
        fs::remove_file(
            checkpoint_file()
        )?;
    }

    // ========================================================
    // DONE
    // ========================================================

    println!();
    println!(
        "======================================================="
    );
    println!(
        "EXTRACTION COMPLETED SUCCESSFULLY"
    );
    println!(
        "======================================================="
    );
    println!(
        "Chain       : {}",
        chain
    );
    println!(
        "Start Block : {}",
        start_block
    );
    println!(
        "End Block   : {}",
        end_block
    );
    println!(
        "Output      : ./output/"
    );
    println!(
        "======================================================="
    );

    Ok(())
}
