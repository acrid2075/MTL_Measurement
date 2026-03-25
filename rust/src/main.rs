use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufReader};
use std::path::Path;

use flate2::read::GzDecoder;
use byteorder::{BigEndian, ByteOrder};
use csv::Writer;

mod lib1_framer;
mod lib2_parser;
mod lib3_ob;

use lib1_framer::MsgStream;
use lib2_parser::parse_message;
use lib3_ob::OrderBook;

/// Align ITCH/locate tickers with Compustat-style `tic` in sp500.csv:
/// trim BOM/whitespace, drop accidental spaces, map hyphen and unicode dashes to `.` (BRK-B ↔ BRK.B).
fn normalize_ticker(t: &str) -> String {
    t.trim()
        .trim_start_matches('\u{feff}')
        .chars()
        .filter(|c| !c.is_whitespace())
        .map(|c| match c {
            '-' | '\u{2013}' | '\u{2014}' | '\u{2212}' => '.',
            c => c.to_ascii_uppercase(),
        })
        .collect()
}

fn load_allowed_tickers(date: &str) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
    if date.len() != 8 || !date.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("date must be YYYYMMDD, got {}", date).into());
    }
    let year = &date[..4];
    let sp500_path = "/home/users/swarnick/sp500.csv";

    let mut rdr = csv::Reader::from_path(sp500_path)?;
    let mut allowed = HashSet::new();

    for result in rdr.records() {
        let record = result?;
        let datadate = record.get(3).unwrap_or("").trim();   // datadate
        let tic = record.get(4).unwrap_or("").trim();        // tic
        let flag = record.get(5).unwrap_or("").trim();       // curr_sp500_flag

        if flag == "1" && datadate.starts_with(year) && !tic.is_empty() {
            allowed.insert(normalize_ticker(tic));
        }
    }

    Ok(allowed)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    // ---------------------------------------------
    // CLI: cargo run --release -- 20180102
    // ---------------------------------------------
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} YYYYMMDD", args[0]);
        std::process::exit(1);
    }

    let date = &args[1];
    if date.len() != 8 || !date.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("date must be YYYYMMDD, got {}", date).into());
    }

    // ---------------------------------------------
    // Paths (project-root relative)
    // ---------------------------------------------
    let yy = &date[2..4];
    let mm = &date[4..6];
    let dd = &date[6..8];
    let itch_date = format!("{}{}{}", mm, dd, yy);

    let itch_path = format!("/work/projects/nasdaq/itchdata/S{}-v50.txt.gz", itch_date);
    let locate_path = format!("data/locates/bx_stocklocate_{}.txt", date);
    let lob_root = env::var("MTL_LOB_ROOT")
        .unwrap_or_else(|_| format!("{}/data/lob_files", env::current_dir().unwrap().display()));

    let output_dir = format!("{}/{}", lob_root, date);

    if !Path::new(&itch_path).exists() {
        panic!("ITCH file not found: {}", itch_path);
    }

    if !Path::new(&locate_path).exists() {
        panic!("Locate file not found: {}", locate_path);
    }

    fs::create_dir_all(&output_dir)?;

    println!("Parsing {}", itch_path);

    let mut total_locates = 0usize;
    let mut kept_locates = 0usize;

    // ---------------------------------------------
    // Load locate map (locate -> ticker)
    // ---------------------------------------------
    let allowed_tickers = load_allowed_tickers(date)?;
    if allowed_tickers.is_empty() {
        return Err(format!("No allowed S&P tickers found for year {}", &date[..4]).into());
    }
    println!("Loaded {} allowed S&P tickers for {}", allowed_tickers.len(), &date[..4]);

    let mut loc_to_ticker: HashMap<u16, String> = HashMap::new();

    let locate_file = std::fs::read_to_string(&locate_path)?;
    for line in locate_file.lines() {
        total_locates += 1;

        let mut parts = line.splitn(3, ',');

        let raw_ticker = match parts.next() {
            Some(x) => x.trim(),
            None => continue,
        };

        let locate_str = match parts.next() {
            Some(x) => x.trim(),
            None => continue,
        };

        let locate: u16 = match locate_str.parse() {
            Ok(x) if x != 0 => x,
            _ => continue,
        };

        let ticker = normalize_ticker(raw_ticker);

        if !allowed_tickers.contains(&ticker) {
            continue;
        }

        kept_locates += 1;
        loc_to_ticker.insert(locate, ticker);
    }

    if loc_to_ticker.is_empty() {
        return Err(format!("No locate codes matched allowed tickers for {}", date).into());
    }

    println!(
        "Locate filter: kept {} of {} locate rows",
        kept_locates, total_locates
    );
    println!("Loaded {} filtered locate codes", loc_to_ticker.len());

    // ---------------------------------------------
    // Stream ITCH
    // ---------------------------------------------
    let file = File::open(&itch_path)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut stream = MsgStream::from_reader(reader);

    let mut books: HashMap<u16, OrderBook> = HashMap::new();
    let mut writers: HashMap<u16, Writer<File>> = HashMap::new();

    let mut frame_count = 0usize;

    while let Some(frame) = stream.next_frame()? {

        frame_count += 1;

        // Only process message types relevant to book
        let msg_type = frame[0];
        match msg_type {
            b'A' | b'F' | b'C' | b'D' | b'E' | b'P' | b'U' | b'X' => {}
            _ => continue,
        }

        let locate = BigEndian::read_u16(&frame[1..3]);

        // Skip non-equity / unwanted
        let ticker = match loc_to_ticker.get(&locate) {
            Some(t) => t,
            None => continue,
        };

        let mut msgs = parse_message(frame)?;

        let book = books.entry(locate)
            .or_insert_with(OrderBook::new);

        book.process_message(&mut msgs)?;

        // Write each resulting message
        for msg in msgs {

            let writer = writers.entry(locate).or_insert_with(|| {
                let path = format!("{}/{}_{}.csv", output_dir, ticker, locate);
                Writer::from_path(path).unwrap()
            });

            writer.serialize(msg)?;
        }
    }

    for (_, mut w) in writers {
        w.flush()?;
    }


    println!("Finished. Frames processed: {}", frame_count);

    Ok(())
}
