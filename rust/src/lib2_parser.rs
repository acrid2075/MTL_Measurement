use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;
use std::io::Result;
use std::u32;
use smallvec::SmallVec;
// monthdayyear

#[derive(Serialize, Clone, Copy)]
pub struct Message {
    pub typ: u8,
    pub timestamp: u64,
    pub orn: u64,
    pub buy_sell: u8,
    pub shares: u32,
    pub price: u32,
    pub bid: u32,
    pub ask: u32,
    pub spread: u32,
    pub ask_depth: u32,
    pub bid_depth: u32,
    pub depth: u32,
}

pub fn parse_message(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    let msg_type = frame[0];

    match msg_type {
        b'U' => parse_U(frame),
        b'X' => parse_X(frame),
        b'A' => parse_A(frame),
        b'P' => parse_P(frame),
        b'E' => parse_E(frame),
        b'C' => parse_C(frame),
        b'D' => parse_D(frame),
        b'F' => parse_F(frame),
        // b'S' => parse_S(frame),
        // b'R' => parse_R(frame),
        // b'H' => parse_H(frame),
        // b'Y' => parse_Y(frame)
        // b'L' => parse_L(frame),
        // b'V' => parse_V(frame),
        // b'W' => parse_W(frame),
        // b'J' => parse_J(frame),
        // b'h' => parse_h(frame),
        // b'Q' => parse_Q(frame),
        // b'B' => parse_B(frame),
        // b'I' => parse_I(frame),
        // b'N' => parse_N(frame),
        _ => Ok(SmallVec::new()),
        // _ => Err(std::io::Error::new(
        //     std::io::ErrorKind::InvalidData,
        //     "Unknown message",
        // )),
    }
}

fn read_u48_be(x: &[u8]) -> u64 {
    ((x[0] as u64) << 40) |
    ((x[1] as u64) << 32) |
    ((x[2] as u64) << 24) |
    ((x[3] as u64) << 16) |
    ((x[4] as u64) << 8)  |
    (x[5] as u64)
}

fn parse_X(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let cancelled_shares = BigEndian::read_u32(&frame[19..23]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'X',
        timestamp: timestamp,
        orn: orn,
        buy_sell: 0,
        shares: cancelled_shares,
        price: 0,
    });
    Ok(v)
}

fn parse_A(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let buy_sell = frame[19];
    let shares = BigEndian::read_u32(&frame[20..24]);
    // stock information 8
    let price = BigEndian::read_u32(&frame[32..36]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'A',
        timestamp: timestamp,
        orn: orn,
        buy_sell: buy_sell,
        shares: shares,
        price: price,
    });
    Ok(v)
}

fn parse_P(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]) // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let buy_sell = frame[19];
    let shares = BigEndian::read_u32(&frame[20..24]);
    // stock information 8
    let price = BigEndian::read_u32(&frame[32..36]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'P',
        timestamp: timestamp,
        orn: orn,
        buy_sell: buy_sell,
        shares: shares,
        price: price,
    });
    Ok(v)
}

fn parse_U(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let old_orn = BigEndian::read_u64(&frame[11..19]); //8
    let new_orn = BigEndian::read_u64(&frame[19..27]); //8
    let shares = BigEndian::read_u32(&frame[27..31]);
    let price = BigEndian::read_u32(&frame[31..35]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'U',
        timestamp: timestamp,
        orn: old_orn,
        buy_sell: 0,
        shares: shares,
        price: 0,
    });
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'A',
        timestamp: timestamp,
        orn: new_orn,
        buy_sell: 0,
        shares: shares,
        price: price,
    });
    Ok(v)
}

fn parse_E(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let executed_shares = BigEndian::read_u32(&frame[19..23]);
    // match number 9
    // let executed_price = BigEndian::read_u32(&frame[32..36]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'E',
        timestamp: timestamp,
        orn: orn,
        buy_sell: 0,
        shares: executed_shares,
        price: 0,//executed_price,
    });
    Ok(v)
}

fn parse_D(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'D',
        timestamp: timestamp,
        orn: orn,
        buy_sell: 0,
        shares: 0,
        price: 0,
    });
    Ok(v)
}

fn parse_C(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]) // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let executed_shares = BigEndian::read_u32(&frame[19..23]);
    // match number 9
    let price = BigEndian::read_u32(&frame[32..36]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'C',
        timestamp: timestamp,
        orn: orn,
        buy_sell: 0,
        shares: executed_shares,
        price: price,
    });
    Ok(v)
}

fn parse_F(frame: &[u8]) -> std::io::Result<SmallVec<[Message; 2]>> {
    // let locate = BigEndian::read_u16(&frame[1..3]); // 2
    // tracking number 2
    let timestamp = read_u48_be(&frame[5..11]); //6
    let orn = BigEndian::read_u64(&frame[11..19]); //8
    let buy_sell = frame[19];
    let shares = BigEndian::read_u32(&frame[20..24]);
    // stock information 8
    let price = BigEndian::read_u32(&frame[32..36]);
    let mut v = SmallVec::<[Message; 2]>::new();
    v.push(Message {
        bid_depth: 0,
        ask_depth: 0,
        depth: 0,
        spread: 0,
        bid: 0,
        ask: 0,
        typ: b'F',
        timestamp: timestamp,
        orn: orn,
        buy_sell: buy_sell,
        shares: shares,
        price: price,
    });
    Ok(v)
}
