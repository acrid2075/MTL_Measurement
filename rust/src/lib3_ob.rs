use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Result;
use std::u32;
use smallvec::SmallVec;
use crate::lib2_parser::Message;
// monthdayyear

pub struct OrderBook {
    //[price, shares]
    pub bids: HashMap<u64, [u32; 2]>,
    pub asks: HashMap<u64, [u32; 2]>,
    //{price: shares}
    pub bid_spread: BTreeMap<u32, u32>,
    pub ask_spread: BTreeMap<u32, u32>,
    pub bid_depth: u32,
    pub ask_depth: u32,
}

impl OrderBook {
    pub fn new() -> OrderBook {
        OrderBook {
            bids: HashMap::new(),
            asks: HashMap::new(),
            //{price: shares}
            bid_spread: BTreeMap::new(),
            ask_spread: BTreeMap::new(),
            bid_depth: 0,
            ask_depth: 0,
        }
    }

    pub fn process_message(&mut self, msgs: &mut SmallVec<[Message; 2]>) -> Result<()> {
        //a,f,d,x,c,e,u
        let msg0 = &mut msgs[0];
        let typ = msg0.typ;
        if typ == b'A' || typ == b'F' {
            //a,f
            let shares = msg0.shares;
            let price = msg0.price;
            if msg0.buy_sell == b'B' {
                self.bids.insert(msg0.orn, [price, shares]);
                self.bid_depth += shares;
                self.bid_spread
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares += shares)
                    .or_insert(shares);
            } else {
                self.asks.insert(msg0.orn, [price, shares]);
                self.ask_depth += shares;
                self.ask_spread
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares += shares)
                    .or_insert(shares);
            }
        } else if typ == b'D' || typ == b'U' {
            //d, u
            if let Some(ord) = self.bids.remove(&msg0.orn) { //bids is hashmap
                let shares = ord[1];
                let price = ord[0]; //price is the price associated with orn
                self.bid_spread //bid_spread is the binary tree
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares -= shares);
                if self.bid_spread.get(&price).unwrap() == &0 {
                    self.bid_spread.remove(&price);
                }
                msg0.buy_sell = b'B';
                msg0.price = price;
                if typ == b'D' {
                    msg0.shares = shares;
                }
                self.bid_depth -= shares;
            } else if let Some(ord) = self.asks.remove(&msg0.orn) {
                let shares = ord[1];
                let price = ord[0];
                self.ask_spread
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares -= shares);
                if self.ask_spread.get(&price).unwrap() == &0 {
                    self.ask_spread.remove(&price);
                }
                msg0.buy_sell = b'S';
                msg0.price = price;
                if typ == b'D' {
                    msg0.shares = shares;
                }
                self.ask_depth -= shares;
            }
        } else if typ == b'C' || typ == b'E' || typ == b'X' {
            //c,e,x
            if self.bids.contains_key(&msg0.orn) {
                let ord = self.bids.get(&msg0.orn).unwrap();
                let shares = msg0.shares;
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if shares_rem == 0 {
                    self.bids.remove(&msg0.orn);
                } else {
                    self.bids.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                self.bid_spread
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares -= shares);
                if self.bid_spread.get(&price).unwrap() == &0 {
                    self.bid_spread.remove(&price);
                }
                self.bid_depth -= shares;
                msg0.buy_sell = b'B';
                msg0.price = price;
            } else if self.asks.contains_key(&msg0.orn) {
                let ord = self.asks.get(&msg0.orn).unwrap();
                let shares = msg0.shares;
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if shares_rem == 0 {
                    self.asks.remove(&msg0.orn);
                } else {
                    self.asks.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                self.ask_spread
                    .entry(price)
                    .and_modify(|curr_shares| *curr_shares -= shares);
                if self.ask_spread.get(&price).unwrap() == &0 {
                    self.ask_spread.remove(&price);
                }
                self.ask_depth -= shares;
                msg0.buy_sell = b'S';
                msg0.price = price;
            }
        }

        let bid = if let Some((bpr, _bshr)) = self.bid_spread.last_key_value() {
            *bpr
        } else {
            0
        };
        let ask = if let Some((apr, _ashr)) = self.ask_spread.first_key_value() {
            *apr
        } else {
            0
        };

        msg0.bid = bid;
        msg0.ask = ask;
        msg0.spread = if ask >= bid { ask - bid } else { 0 };
        msg0.ask_depth = self.ask_depth;
        msg0.bid_depth = self.bid_depth;
        msg0.depth = self.ask_depth + self.bid_depth;
        
        if typ == b'U' {
            let msg1 = &mut msgs[1];
            msg1.bid = bid;
            msg1.ask = ask;
            msg1.ask_depth = self.ask_depth;
            msg1.bid_depth = self.bid_depth;
            msg1.depth = self.ask_depth + self.bid_depth;
            msg1.buy_sell = msg0.buy_sell;
        }

        Ok()
    }
}