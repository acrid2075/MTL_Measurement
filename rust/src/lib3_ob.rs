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
    fn decrement_level(
        levels: &mut BTreeMap<u32, u32>,
        price: u32,
        shares: u32,
        side: &str,
        orn: u64,
    ) -> u32 {
        let mut removed = 0;
        let mut should_remove = false;
        if let Some(curr) = levels.get_mut(&price) {
            removed = shares.min(*curr);
            if shares > *curr {
                eprintln!(
                    "WARN: {} level underflow risk for orn={} price={} requested={} available={}",
                    side, orn, price, shares, *curr
                );
            }
            *curr -= removed;
            should_remove = *curr == 0;
        } else {
            eprintln!(
                "WARN: missing {} price level for orn={} price={} shares={}",
                side, orn, price, shares
            );
        }

        if should_remove {
            levels.remove(&price);
        }

        removed
    }

    fn apply_add(&mut self, buy_sell: u8, orn: u64, price: u32, shares: u32) {
        if buy_sell == b'B' {
            self.bids.insert(orn, [price, shares]);
            self.bid_depth = self.bid_depth.saturating_add(shares);
            self.bid_spread
                .entry(price)
                .and_modify(|curr_shares| *curr_shares = curr_shares.saturating_add(shares))
                .or_insert(shares);
        } else if buy_sell == b'S' {
            self.asks.insert(orn, [price, shares]);
            self.ask_depth = self.ask_depth.saturating_add(shares);
            self.ask_spread
                .entry(price)
                .and_modify(|curr_shares| *curr_shares = curr_shares.saturating_add(shares))
                .or_insert(shares);
        } else {
            eprintln!(
                "WARN: invalid side on add orn={} side={} shares={} price={}",
                orn, buy_sell, shares, price
            );
        }
    }

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
            self.apply_add(msg0.buy_sell, msg0.orn, msg0.price, msg0.shares);
        } else if typ == b'D' || typ == b'U' {
            //d, u
            if let Some(ord) = self.bids.remove(&msg0.orn) { //bids is hashmap
                let shares = ord[1];
                let price = ord[0]; //price is the price associated with orn
                let removed = Self::decrement_level(&mut self.bid_spread, price, shares, "bid", msg0.orn);
                msg0.buy_sell = b'B';
                msg0.price = price;
                if typ == b'D' {
                    msg0.shares = removed;
                }
                self.bid_depth = self.bid_depth.saturating_sub(removed);
            } else if let Some(ord) = self.asks.remove(&msg0.orn) {
                let shares = ord[1];
                let price = ord[0];
                let removed = Self::decrement_level(&mut self.ask_spread, price, shares, "ask", msg0.orn);
                msg0.buy_sell = b'S';
                msg0.price = price;
                if typ == b'D' {
                    msg0.shares = removed;
                }
                self.ask_depth = self.ask_depth.saturating_sub(removed);
            } else {
                eprintln!("WARN: remove/update for missing orn={} typ={}", msg0.orn, typ);
            }
        } else if typ == b'E' || typ == b'X' {
            //c,e,x
            if self.bids.contains_key(&msg0.orn) {
                let ord = self.bids.get(&msg0.orn).unwrap();
                let requested = msg0.shares;
                let shares = requested.min(ord[1]);
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if requested > ord[1] {
                    eprintln!(
                        "WARN: bid reduction exceeds shares orn={} requested={} available={}",
                        msg0.orn, requested, ord[1]
                    );
                }
                if shares_rem == 0 {
                    self.bids.remove(&msg0.orn);
                } else {
                    self.bids.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                let removed = Self::decrement_level(&mut self.bid_spread, price, shares, "bid", msg0.orn);
                self.bid_depth = self.bid_depth.saturating_sub(removed);
                msg0.buy_sell = b'B';
                msg0.price = price;
                msg0.shares = removed;
            } else if self.asks.contains_key(&msg0.orn) {
                let ord = self.asks.get(&msg0.orn).unwrap();
                let requested = msg0.shares;
                let shares = requested.min(ord[1]);
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if requested > ord[1] {
                    eprintln!(
                        "WARN: ask reduction exceeds shares orn={} requested={} available={}",
                        msg0.orn, requested, ord[1]
                    );
                }
                if shares_rem == 0 {
                    self.asks.remove(&msg0.orn);
                } else {
                    self.asks.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                let removed = Self::decrement_level(&mut self.ask_spread, price, shares, "ask", msg0.orn);
                self.ask_depth = self.ask_depth.saturating_sub(removed);
                msg0.buy_sell = b'S';
                msg0.price = price;
                msg0.shares = removed;
            } else {
                eprintln!("WARN: execution/cancel for missing orn={} typ={}", msg0.orn, typ);
            }
        } else if typ == b'C' {
            //c,e,x
            if self.bids.contains_key(&msg0.orn) {
                let ord = self.bids.get(&msg0.orn).unwrap();
                let requested = msg0.shares;
                let shares = requested.min(ord[1]);
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if requested > ord[1] {
                    eprintln!(
                        "WARN: bid reduction exceeds shares orn={} requested={} available={}",
                        msg0.orn, requested, ord[1]
                    );
                }
                if shares_rem == 0 {
                    self.bids.remove(&msg0.orn);
                } else {
                    self.bids.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                let removed = Self::decrement_level(&mut self.bid_spread, price, shares, "bid", msg0.orn);
                self.bid_depth = self.bid_depth.saturating_sub(removed);
                msg0.buy_sell = b'B';
                msg0.shares = removed;
            } else if self.asks.contains_key(&msg0.orn) {
                let ord = self.asks.get(&msg0.orn).unwrap();
                let requested = msg0.shares;
                let shares = requested.min(ord[1]);
                let shares_rem = ord[1] - shares;
                let price = ord[0];
                if requested > ord[1] {
                    eprintln!(
                        "WARN: ask reduction exceeds shares orn={} requested={} available={}",
                        msg0.orn, requested, ord[1]
                    );
                }
                if shares_rem == 0 {
                    self.asks.remove(&msg0.orn);
                } else {
                    self.asks.entry(msg0.orn).and_modify(|ord| ord[1] = shares_rem);
                }
                let removed = Self::decrement_level(&mut self.ask_spread, price, shares, "ask", msg0.orn);
                self.ask_depth = self.ask_depth.saturating_sub(removed);
                msg0.buy_sell = b'S';
                msg0.shares = removed;
            } else {
                eprintln!("WARN: execution/cancel for missing orn={} typ={}", msg0.orn, typ);
            }
        }

        if typ == b'U' {
            let msg1 = &mut msgs[1];
            if msg0.buy_sell == b'B' || msg0.buy_sell == b'S' {
                msg1.buy_sell = msg0.buy_sell;
                self.apply_add(msg1.buy_sell, msg1.orn, msg1.price, msg1.shares);
            } else {
                eprintln!(
                    "WARN: replacement add skipped due to unknown side old_orn={} new_orn={}",
                    msg0.orn, msg1.orn
                );
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
            let buy_sell = msg0.buy_sell;
            let msg1 = &mut msgs[1];
            msg1.bid = bid;
            msg1.ask = ask;
            msg1.ask_depth = self.ask_depth;
            msg1.bid_depth = self.bid_depth;
            msg1.depth = self.ask_depth + self.bid_depth;
            msg1.buy_sell = buy_sell;
        }

        Ok(())
    }
}