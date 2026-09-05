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
// RPC CONFIGURATION
// ============================================================

fn get_rpc_list(
    chain: &str,
) -> Result<Vec<String>, String> {

    let mut rpc_list: Vec<String> = Vec::new();


    // --------------------------------------------------------
    // USER PROVIDED RPC
    // --------------------------------------------------------

    if let Ok(custom_rpc) = env::var("RPC_URL") {

        let custom_rpc =
            custom_rpc.trim();

        if !custom_rpc.is_empty() {

            rpc_list.push(
                custom_rpc.to_string()
            );
        }
    }


    // --------------------------------------------------------
    // DEFAULT PUBLIC RPCs
    // --------------------------------------------------------

    let defaults: Vec<String> = match chain {

        // ====================================================
        // BNB SMART CHAIN
        // ====================================================

        "bnb" => vec![
            "https://bsc-dataseed.binance.org/"
                .to_string(),

            "https://bsc-dataseed1.defibit.io/"
                .to_string(),

            "https://bsc-dataseed1.ninicoin.io/"
                .to_string(),

            "https://rpc.ankr.com/bsc"
                .to_string(),

            "https://1rpc.io/bnb"
                .to_string(),
        ],


        // ====================================================
        // ETHEREUM
        // ====================================================

        "ethereum" => vec![
            "https://eth.llamarpc.com"
                .to_string(),

            "https://cloudflare-eth.com"
                .to_string(),

            "https://rpc.ankr.com/eth"
                .to_string(),

            "https://1rpc.io/eth"
                .to_string(),
        ],


        // ====================================================
        // POLYGON
        // ====================================================

        "polygon" => vec![
            "https://polygon-rpc.com"
                .to_string(),

            "https://rpc.ankr.com/polygon"
                .to_string(),

            "https://1rpc.io/matic"
                .to_string(),
        ],


        // ====================================================
        // ARBITRUM
        // ====================================================

        "arbitrum" => vec![
            "https://arb1.arbitrum.io/rpc"
                .to_string(),

            "https://rpc.ankr.com/arbitrum"
                .to_string(),

            "https://1rpc.io/arb"
                .to_string(),
        ],


        // ====================================================
        // BASE
        // ====================================================

        "base" => vec![
            "https://mainnet.base.org"
                .to_string(),

            "https://base.llamarpc.com"
                .to_string(),

            "https://1rpc.io/base"
                .to_string(),
        ],


        // ====================================================
        // OPTIMISM
        // ====================================================

        "optimism" => vec![
            "https://mainnet.optimism.io"
                .to_string(),

            "https://optimism.llamarpc.com"
                .to_string(),

            "https://1rpc.io/op"
                .to_string(),
        ],


        // ====================================================
        // AVALANCHE C-CHAIN
        // ====================================================

        "avalanche_c" => vec![
            "https://api.avax.network/ext/bc/C/rpc"
                .to_string(),

            "https://rpc.ankr.com/avalanche"
                .to_string(),

            "https://1rpc.io/avax/c"
                .to_string(),
        ],


        // ====================================================
        // INVALID CHAIN
        // ====================================================

        _ => {
            return Err(
                format!(
                    "Unsupported chain: {}",
                    chain
                )
            );
        }
    };


    // --------------------------------------------------------
    // ADD DEFAULT RPCs WITHOUT DUPLICATES
    // --------------------------------------------------------

    for rpc in defaults {

        if !rpc_list.contains(&rpc) {

            rpc_list.push(rpc);
        }
    }


    Ok(rpc_list)
}


// ============================================================
// HEX → U64
// ============================================================

fn hex_to_u64(
    value: &str,
) -> Option<u64> {

    let value =
        value.trim_start_matches("0x");

    u64::from_str_radix(
        value,
        16,
    )
    .ok()
}


// ============================================================
// GENERIC RPC CALL
// ============================================================

async fn rpc_call(
    client: &reqwest::Client,
    rpc_urls: &[String],
    payload: Value,
) -> Result<Value, String> {

    let mut last_error =
        String::from(
            "Unknown RPC error"
        );


    for rpc in rpc_urls {

        for attempt in 1..=MAX_RPC_RETRIES {

            match client
                .post(rpc)
                .json(&payload)
                .send()
                .await
            {

                // ------------------------------------------------
                // HTTP RESPONSE
                // ------------------------------------------------

                Ok(response) => {

                    let status =
                        response.status();


                    if !status.is_success() {

                        last_error =
                            format!(
                                "{} returned HTTP {}",
                                rpc,
                                status
                            );

                    } else {

                        // ----------------------------------------
                        // JSON
                        // ----------------------------------------

                        match response
                            .json::<Value>()
                            .await
                        {

                            Ok(value) => {

                                if value
                                    .get("error")
                                    .is_some()
                                {

                                    last_error =
                                        format!(
                                            "{} returned RPC error: {}",
                                            rpc,
                                            value["error"]
                                        );

                                } else {

                                    return Ok(value);
                                }
                            }


                            Err(error) => {

                                last_error =
                                    format!(
                                        "{} returned invalid JSON: {}",
                                        rpc,
                                        error
                                    );
                            }
                        }
                    }
                }


                // ------------------------------------------------
                // REQUEST ERROR
                // ------------------------------------------------

                Err(error) => {

                    last_error =
                        format!(
                            "{} request failed: {}",
                            rpc,
                            error
                        );
                }
            }


            // ----------------------------------------------------
            // RETRY DELAY
            // ----------------------------------------------------

            if attempt < MAX_RPC_RETRIES {

                sleep(
                    Duration::from_millis(
                        300 * attempt as u64
                    )
                )
                .await;
            }
        }


        eprintln!(
            "RPC failed. Switching to next RPC: {}",
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


    let response =
        rpc_call(
            client,
            rpc_urls,
            payload
        )
        .await?;


    let result =
        response
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                "Missing eth_blockNumber result"
                    .to_string()
            })?;


    hex_to_u64(result)
        .ok_or_else(|| {
            "Invalid latest block number"
                .to_string()
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


    let response =
        rpc_call(
            client,
            rpc_urls,
            payload
        )
        .await?;


    let result =
        response
            .get("result")
            .ok_or_else(|| {
                format!(
                    "Block {} has no result",
                    block
                )
            })?;


    if result.is_null() {

        return Err(
            format!(
                "Block {} returned null",
                block
            )
        );
    }


    let timestamp_hex =
        result
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

        let mid =
            low + (high - low) / 2;


        let timestamp =
            get_block_timestamp(
                client,
                rpc_urls,
                mid,
            )
            .await?;


        if timestamp < target_timestamp {

            low =
                mid + 1;

        } else {

            high =
                mid;
        }
    }


    Ok(low)
}


// ============================================================
// FETCH TRANSACTION ADDRESSES FROM BATCH
// ============================================================

async fn fetch_rpc_batch_blocks(
    client: &reqwest::Client,
    rpc_urls: &[String],
    blocks: Vec<u64>,
) -> Result<Vec<String>, String> {

    if blocks.is_empty() {

        return Ok(
            Vec::new()
        );
    }


    // --------------------------------------------------------
    // JSON-RPC BATCH
    // --------------------------------------------------------

    let payload: Vec<Value> =
        blocks
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
        String::from(
            "Batch request failed"
        );


    // --------------------------------------------------------
    // RPC FALLBACK
    // --------------------------------------------------------

    for rpc in rpc_urls {

        for attempt in 1..=MAX_RPC_RETRIES {

            match client
                .post(rpc)
                .json(&payload)
                .send()
                .await
            {

                // --------------------------------------------
                // HTTP
                // --------------------------------------------

                Ok(response) => {

                    if !response
                        .status()
                        .is_success()
                    {

                        last_error =
                            format!(
                                "{} returned HTTP {}",
                                rpc,
                                response.status()
                            );

                    } else {

                        // ------------------------------------
                        // JSON ARRAY
                        // ------------------------------------

                        match response
                            .json::<Vec<Value>>()
                            .await
                        {

                            Ok(results) => {

                                let mut addresses:
                                    Vec<String> =
                                    Vec::new();


                                let mut received:
                                    HashSet<u64> =
                                    HashSet::new();


                                // --------------------------------
                                // PROCESS BLOCKS
                                // --------------------------------

                                for item in results {

                                    // ----------------------------
                                    // RPC ERROR
                                    // ----------------------------

                                    if item
                                        .get("error")
                                        .is_some()
                                    {

                                        last_error =
                                            format!(
                                                "{} returned RPC error: {}",
                                                rpc,
                                                item["error"]
                                            );

                                        continue;
                                    }


                                    // ----------------------------
                                    // RESULT
                                    // ----------------------------

                                    let result =
                                        match item
                                            .get("result")
                                        {

                                            Some(value)
                                                if !value.is_null() =>
                                            {
                                                value
                                            }

                                            _ => {
                                                continue;
                                            }
                                        };


                                    // ----------------------------
                                    // BLOCK NUMBER
                                    // ----------------------------

                                    if let Some(number) =
                                        result
                                            .get("number")
                                            .and_then(|v| v.as_str())
                                            .and_then(hex_to_u64)
                                    {

                                        received.insert(
                                            number
                                        );
                                    }


                                    // ----------------------------
                                    // TRANSACTIONS
                                    // ----------------------------

                                    if let Some(transactions) =
                                        result
                                            .get("transactions")
                                            .and_then(|v| v.as_array())
                                    {

                                        for tx in transactions {

                                            // --------------------
                                            // FROM
                                            // --------------------

                                            if let Some(from) =
                                                tx
                                                    .get("from")
                                                    .and_then(|v| v.as_str())
                                            {

                                                addresses.push(
                                                    from
                                                        .to_ascii_lowercase()
                                                );
                                            }


                                            // --------------------
                                            // TO
                                            // --------------------

                                            if let Some(to) =
                                                tx
                                                    .get("to")
                                                    .and_then(|v| v.as_str())
                                            {

                                                addresses.push(
                                                    to
                                                        .to_ascii_lowercase()
                                                );
                                            }
                                        }
                                    }
                                }


                                // --------------------------------
                                // VERIFY COMPLETE RESPONSE
                                // --------------------------------

                                if received.len()
                                    == blocks.len()
                                {

                                    return Ok(
                                        addresses
                                    );
                                }


                                last_error =
                                    format!(
                                        "{} returned only {}/{} requested blocks",
                                        rpc,
                                        received.len(),
                                        blocks.len()
                                    );
                            }


                            Err(error) => {

                                last_error =
                                    format!(
                                        "{} returned invalid JSON: {}",
                                        rpc,
                                        error
                                    );
                            }
                        }
                    }
                }


                // --------------------------------------------
                // REQUEST ERROR
                // --------------------------------------------

                Err(error) => {

                    last_error =
                        format!(
                            "{} request failed: {}",
                            rpc,
                            error
                        );
                }
            }


            // ------------------------------------------------
            // RETRY
            // ------------------------------------------------

            if attempt < MAX_RPC_RETRIES {

                sleep(
                    Duration::from_millis(
                        500 * attempt as u64
                    )
                )
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
// CHECKPOINT FILE
// ============================================================

fn checkpoint_file() -> &'static str {
    "output/checkpoint.txt"
}


// ============================================================
// SAVE CHECKPOINT
// ============================================================

fn save_checkpoint(
    block: u64,
) -> Result<(), Box<dyn std::error::Error>> {

    let temporary =
        "output/checkpoint.tmp";


    fs::write(
        temporary,
        block.to_string()
    )?;


    fs::rename(
        temporary,
        checkpoint_file()
    )?;


    Ok(())
}


// ============================================================
// LOAD CHECKPOINT
// ============================================================

fn load_checkpoint() -> Option<u64> {

    if !Path::new(
        checkpoint_file()
    )
    .exists()
    {
        return None;
    }


    let content =
        fs::read_to_string(
            checkpoint_file()
        )
        .ok()?;


    content
        .trim()
        .parse::<u64>()
        .ok()
}


// ============================================================
// FIND NEXT PART NUMBER
// ============================================================

fn next_part_number(
    chain: &str,
) -> u32 {

    let mut part =
        1u32;


    loop {

        let path =
            format!(
                "output/{}_addresses_1M_part_{:03}.csv.gz",
                chain,
                part
            );


        if !Path::new(
            &path
        )
        .exists()
        {
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
        File::create(
            &file_name
        )?;


    let encoder =
        GzEncoder::new(
            file,
            Compression::default()
        );


    let mut writer =
        csv::Writer::from_writer(
            encoder
        );


    // --------------------------------------------------------
    // HEADER
    // --------------------------------------------------------

    writer.write_record(
        ["address"]
    )?;


    // --------------------------------------------------------
    // ADDRESSES
    // --------------------------------------------------------

    for address in addresses {

        writer.write_record(
            [address]
        )?;
    }


    writer.flush()?;


    // --------------------------------------------------------
    // FINISH CSV
    // --------------------------------------------------------

    let encoder =
        writer
            .into_inner()
            .map_err(|error| {
                error.into_error()
            })?;


    // --------------------------------------------------------
    // FINISH GZIP
    // --------------------------------------------------------

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
    // READ ENVIRONMENT
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
                |value| {
                    value
                        .parse::<u64>()
                        .ok()
                }
            );


    let mut end_ts =
        env::var("END_TIMESTAMP")
            .ok()
            .and_then(
                |value| {
                    value
                        .parse::<u64>()
                        .ok()
                }
            );


    // ========================================================
    // READ CLI ARGUMENTS
    // ========================================================

    let args:
        Vec<String> =
        env::args().collect();


    let mut index =
        1usize;


    while index < args.len() {

        match args[index].as_str() {

            // ------------------------------------------------
            // CHAIN
            // ------------------------------------------------

            "--chain" => {

                index += 1;

                if index < args.len() {

                    chain =
                        args[index]
                            .clone();
                }
            }


            // ------------------------------------------------
            // START TIMESTAMP
            // ------------------------------------------------

            "--start-ts" => {

                index += 1;

                if index < args.len() {

                    start_ts =
                        Some(
                            args[index]
                                .parse::<u64>()?
                        );
                }
            }


            // ------------------------------------------------
            // END TIMESTAMP
            // ------------------------------------------------

            "--end-ts" => {

                index += 1;

                if index < args.len() {

                    end_ts =
                        Some(
                            args[index]
                                .parse::<u64>()?
                        );
                }
            }


            _ => {}
        }


        index += 1;
    }


    // ========================================================
    // VALIDATE TIMESTAMPS
    // ========================================================

    let start_ts =
        start_ts.ok_or(
            "Missing start timestamp"
        )?;


    let end_ts =
        end_ts.ok_or(
            "Missing end timestamp"
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

    fs::create_dir_all(
        "output"
    )?;


    // ========================================================
    // RPC LIST
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
    // STARTUP INFORMATION
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
        "Chain            : {}",
        chain
    );

    println!(
        "Start Timestamp  : {}",
        start_ts
    );

    println!(
        "End Timestamp    : {}",
        end_ts
    );

    println!(
        "Blocks / Part    : {}",
        BLOCKS_PER_PART
    );

    println!(
        "Blocks / RPC     : {}",
        BLOCKS_PER_RPC_BATCH
    );

    println!(
        "Concurrency      : {}",
        CONCURRENCY
    );

    println!(
        "RPC Endpoints    : {}",
        rpc_list.len()
    );

    println!(
        "======================================================="
    );


    for (
        number,
        rpc,
    ) in rpc_list.iter().enumerate()
    {

        println!(
            "RPC #{}: {}",
            number + 1,
            rpc
        );
    }


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
        Arc::new(
            client
        );


    let rpc_list =
        Arc::new(
            rpc_list
        );


    // ========================================================
    // GET LATEST BLOCK
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


    // ========================================================
    // GET LATEST TIMESTAMP
    // ========================================================

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
    // FUTURE RANGE CHECK
    // ========================================================

    if start_ts >
        latest_timestamp
    {

        return Err(
            format!(
                "Requested start date is in the future. Latest chain timestamp: {}",
                latest_timestamp
            )
            .into()
        );
    }


    // ========================================================
    // FIND START BLOCK
    // ========================================================

    println!();

    println!(
        "Searching START block..."
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
    // FIND END BLOCK
    // ========================================================

    let end_block: u64;


    if end_ts >= latest_timestamp {

        // Requested end date includes the current latest block.
        end_block =
            latest_block;

    } else {

        println!();

        println!(
            "Searching END block..."
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
            if first_after_end > 0 {

                first_after_end - 1

            } else {

                0
            };
    }


    // ========================================================
    // END BLOCK TIMESTAMP
    // ========================================================

    let end_block_timestamp =
        get_block_timestamp(
            &client,
            &rpc_list,
            end_block,
        )
        .await?;


    // ========================================================
    // FINAL DATE → BLOCK RESULT
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


    if start_block <= end_block {

        println!(
            "Total Blocks      : {}",
            end_block -
            start_block +
            1
        );

    } else {

        println!(
            "Total Blocks      : 0"
        );
    }


    println!(
        "======================================================="
    );


    // ========================================================
    // NO BLOCKS
    // ========================================================

    if start_block > end_block {

        println!(
            "No blocks found in requested date range."
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

                println!();

                println!(
                    "Starting from block {}",
                    start_block
                );

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
    // MAIN EXTRACTION LOOP
    // ========================================================

    while current_block <= end_block {

        // ----------------------------------------------------
        // CURRENT 1M CHUNK
        // ----------------------------------------------------

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
            "[PART {:03}] BLOCK {} → {}",
            part_num,
            current_block,
            chunk_end
        );

        println!(
            "======================================================="
        );


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
                    .min(
                        chunk_end
                    );


            micro_chunks.push(
                (block..=micro_end)
                    .collect()
            );


            if micro_end ==
                chunk_end
            {
                break;
            }


            block =
                micro_end + 1;
        }


        let total_batches =
            micro_chunks.len();


        println!(
            "RPC Batches: {}",
            total_batches
        );


        // ----------------------------------------------------
        // UNIQUE ADDRESS SET
        // ----------------------------------------------------

        let mut unique_set:
            HashSet<String> =
            HashSet::new();


        // ----------------------------------------------------
        // CONCURRENT STREAM
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

                            fetch_rpc_batch_blocks(
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
        // PROCESS RESULTS
        // ----------------------------------------------------

        let mut processed_batches:
            usize = 0;


        while let Some(result) =
            stream.next().await
        {

            let addresses =
                result
                    .map_err(
                        |error| {

                            format!(
                                "Worker task failed: {}",
                                error
                            )
                        }
                    )?
                    .map_err(
                        |error| {

                            format!(
                                "RPC batch permanently failed: {}",
                                error
                            )
                        }
                    )?;


            // ------------------------------------------------
            // GLOBAL DEDUP INSIDE PART
            // ------------------------------------------------

            for address in addresses {

                unique_set.insert(
                    address
                );
            }


            processed_batches += 1;


            // ------------------------------------------------
            // PROGRESS
            // ------------------------------------------------

            if processed_batches % 1000 == 0
                || processed_batches ==
                    total_batches
            {

                let percentage =
                    (
                        processed_batches
                            as f64
                    )
                    /
                    (
                        total_batches
                            as f64
                    )
                    *
                    100.0;


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
                "======================================================="
            );

            println!(
                "Blocks            : {} → {}",
                current_block,
                chunk_end
            );

            println!(
                "Unique Addresses  : {}",
                unique_set.len()
            );

            println!(
                "Output File       : {}",
                file_name
            );

            println!(
                "======================================================="
            );

        } else {

            println!(
                "PART {:03}: zero addresses found.",
                part_num
            );
        }


        // ----------------------------------------------------
        // NEXT BLOCK
        // ----------------------------------------------------

        let next_block =
            chunk_end.saturating_add(
                1
            );


        // ----------------------------------------------------
        // SAVE CHECKPOINT
        // ----------------------------------------------------

        save_checkpoint(
            next_block
        )?;


        println!(
            "Checkpoint saved: {}",
            next_block
        );


        // ----------------------------------------------------
        // FINISHED
        // ----------------------------------------------------

        if next_block >
            end_block
        {
            break;
        }


        // ----------------------------------------------------
        // NEXT PART
        // ----------------------------------------------------

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
    // FINAL SUMMARY
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
        "Chain          : {}",
        chain
    );

    println!(
        "Start Block    : {}",
        start_block
    );

    println!(
        "End Block      : {}",
        end_block
    );

    println!(
        "Total Blocks   : {}",
        end_block -
        start_block +
        1
    );

    println!(
        "Output          : ./output/"
    );

    println!(
        "======================================================="
    );


    Ok(())
}
