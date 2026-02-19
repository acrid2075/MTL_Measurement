use std::io::{Read, Result};
// monthdayyear

pub const BUFSIZE: usize = 65536;

pub struct MsgStream<R> {
    reader: R,
    buffer: Vec<u8>,
    cursor: usize,
}

fn message_length(t: u8) -> Option<usize> {
    match t {
        b'X' => Some(1 + 24),
        b'A' => Some(1 + 37),
        b'P' => Some(1 + 45),
        b'U' => Some(1 + 36),
        b'E' => Some(1 + 32),
        b'C' => Some(1 + 37),
        b'D' => Some(1 + 20),
        b'F' => Some(1 + 41),
        b'S' => Some(1 + 13),
        b'R' => Some(1 + 40),
        b'H' => Some(1 + 26),
        b'Y' => Some(1 + 21),
        b'L' => Some(1 + 27),
        b'V' => Some(1 + 36),
        b'W' => Some(1 + 13),
        b'J' => Some(1 + 36),
        b'h' => Some(1 + 22),
        b'Q' => Some(1 + 41),
        b'B' => Some(1 + 20),
        b'I' => Some(1 + 51),
        b'N' => Some(1 + 21),
        _ => None,
    }
}

impl<R: Read> MsgStream<R> {
    pub fn from_reader(reader: R) -> MsgStream<R> {
        MsgStream::new(reader)
    }

    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: Vec::with_capacity(1 << 20),
            cursor: 0,
        }
    }

    pub fn next_frame(&mut self) -> std::io::Result<Option<&[u8]>> {
        loop {
            // Need at least 1 byte for message type
            if self.cursor >= self.buffer.len() {
                self.compact();
                if !self.fill_buffer()? {
                    return Ok(None);
                }
                continue;
            }

            let msg_type = self.buffer[self.cursor];

            let len = match message_length(msg_type) {
                Some(l) => l,
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unknown message type: {}", msg_type),
                    ));
                }
            };

            // Not enough bytes yet
            if self.buffer.len() - self.cursor < len {
                if !self.fill_buffer()? {
                    return Ok(None);
                }
                continue;
            }

            let start = self.cursor;
            let end = start + len;

            self.cursor = end;

            return Ok(Some(&self.buffer[start..end]));
        }
    }

    fn fill_buffer(&mut self) -> std::io::Result<bool> {
        let mut temp = [0u8; BUFSIZE];
        let bytes_read = self.reader.read(&mut temp)?;

        if bytes_read == 0 {
            return Ok(false);
        }

        self.buffer.extend_from_slice(&temp[..bytes_read]);
        Ok(true)
    }

    fn compact(&mut self) {
        if self.cursor > 0 {
            self.buffer.drain(..self.cursor);
            self.cursor = 0;
        }
    }
}