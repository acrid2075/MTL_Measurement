use std::collections::HashMap;
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
use lib2_parser::{parse_message, Message};
use lib3_ob::OrderBook;

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

    // ---------------------------------------------
    // Paths (project-root relative)
    // ---------------------------------------------
    let itch_path = format!("/work/projects/nasdaq/itchdata/S{}-v50.txt.gz", date);
    let locate_path = format!("data/locates/bx_stocklocate_{}.txt",
        format!("{}{}", &date[4..], &date[..4])  // reformat YYYYMMDD -> MMDDYYYY
    );
    let output_dir = format!("data/lob_files/{}", date);

    if !Path::new(&itch_path).exists() {
        panic!("ITCH file not found: {}", itch_path);
    }

    if !Path::new(&locate_path).exists() {
        panic!("Locate file not found: {}", locate_path);
    }

    fs::create_dir_all(&output_dir)?;

    println!("Parsing {}", itch_path);

    // ---------------------------------------------
    // Load locate map (locate -> ticker)
    // ---------------------------------------------
    let mut loc_to_ticker: HashMap<u16, String> = HashMap::new();

    let locate_file = std::fs::read_to_string(&locate_path)?;
    for line in locate_file.lines() {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 2 { continue; }
        let ticker = parts[0].trim().to_string();
        let locate: u16 = parts[1].trim().parse().unwrap_or(0);
        loc_to_ticker.insert(locate, ticker);
    }

    println!("Loaded {} locate codes", loc_to_ticker.len());

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
                let path = format!("{}/{}.csv", output_dir, ticker);
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


// use rust::MsgStream;
// use std::env;
// use std::io::{self};
// //use std::process::Command;
// use std::time::Instant;
// fn main() -> io::Result<()> {
//     let args: Vec<String> = env::args().collect();
//     let start = Instant::now();
//     let accepted_year = &args[1];

//     //get the dates that have already been parsed
//     let mut data_files: Vec<String> = Vec::new();
//     let data_folders = std::fs::read_dir("../data/lob_files")?;
//     for folder in data_folders {
//         let entry = folder?.file_name().into_string().unwrap();
//         data_files.push(entry);
//     }

//     //add the dates that still need to be parsed from that certain year 
//     let mut entries: Vec<String> = Vec::new();
//     let dir = std::fs::read_dir("../data/locates")?;
//     for file in dir {
//         let entry = file?.file_name().into_string().unwrap();
//         let year = entry.get(17..19).unwrap();
//         let date = entry.get(19..23).unwrap();
//         let full_date = date.to_owned() + year;
//         if year == accepted_year && !data_files.contains(&full_date) {
//             entries.push(entry);
//         }
//     }
//     entries.sort();

//     //iterate through dates and parse and make the order book
//     for entry in entries {
//         let year = entry.get(17..19).unwrap();
//         let date = entry.get(19..23).unwrap();
//         let full_date = date.to_owned() + year;
//         println!("{} {}", full_date, accepted_year);

//         let dow30 = vec![
//             "AAPL", "AXP", "BA", "CAT", "CSCO", "CVX", "DIS", "DWDP", "GE", "GS", "HD", "IBM",
//             "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV",
//             "UNH", "UTX", "V", "VZ", "WBA", "WMT", "XOM",
//         ];
//         let tester_read = MsgStream::from_gz_to_buf(format!("itchdata/S{}-v50.txt.gz", &full_date));
//         if tester_read.is_err() {
//             continue;
//         } else {
//             let start_file = Instant::now();
//             //let mut test_read =
//                 //MsgStream::from_gz_to_buf(format!("itchdata/S{}-v50.txt.gz", &full_date)).unwrap();
//             let mut test_read =
//                 MsgStream::from_gz_to_buf(format!("itchdata/S{}-v50.txt.gz", &full_date)).unwrap();
//             let _a = test_read.get_locate_codes(dow30, &full_date);
//             println!("{:?}", &test_read.loc_to_ticker);

//             let _b = test_read.process_bytes();
//             let process_time = Instant::elapsed(&start_file);
//             println!("{:?}", process_time);

//             let _c = test_read.process_order_book();
//             let order_time = Instant::elapsed(&start_file) - process_time;
//             println!("{:?}", order_time);
//             let _d = test_read.write_companies(&full_date);
//             println!("finished date: {}", full_date);
//             std::mem::drop(test_read);
//         }

//     }
//     let finish = Instant::elapsed(&start);
//     println!("{:?}", finish);
//     Ok(())
// }
