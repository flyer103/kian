//! The newline-delimited carrier with §7.6's byte bounds.
//!
//! A line is at most `max_payload_kib` **wire bytes in either
//! direction**, enforced **while scanning for `\n` and before any
//! unbounded buffering or JSON parse** (§7.6). The reader consumes the
//! stream in fixed chunks and keeps only what fits, so a hostile child
//! streaming an endless line without a newline can never grow memory
//! past one chunk plus one bound.

use std::io::{Read, Write};

/// Why a line could not be read within bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineError {
    /// Over the byte bound while scanning for `\n` — `frame_overflow`
    /// (§7.6: immediate quarantine, never a window increment).
    Overflow { scanned: usize, max: usize },
    /// The stream ended without a newline after it (EOF without the
    /// expected reply is a crash, §7.5) or the read failed.
    Io(String),
}

/// Bounded line writer (S→P direction).
pub fn write_line<W: Write>(out: &mut W, line: &str, max: usize) -> Result<(), LineError> {
    if line.len() > max {
        return Err(LineError::Overflow {
            scanned: line.len(),
            max,
        });
    }
    out.write_all(line.as_bytes())
        .and_then(|_| out.write_all(b"\n"))
        .and_then(|_| out.flush())
        .map_err(|e| LineError::Io(e.to_string()))
}

/// Bounded line reader (P→S direction): scans for `\n` in fixed-size
/// chunks, refusing to buffer past `max` bytes without one.
pub struct BoundedLineReader<R: Read> {
    inner: R,
    /// Bytes read past the last newline, not yet returned.
    pending: Vec<u8>,
    /// Fixed chunk buffer for the current scan.
    chunk: Vec<u8>,
}

const CHUNK: usize = 8 * 1024;

impl<R: Read> BoundedLineReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            pending: Vec::new(),
            chunk: vec![0; CHUNK],
        }
    }

    /// Reads one `\n`-terminated line (returned without the newline,
    /// as a lossy UTF-8 string — the carrier is UTF-8 JSON, §7.1).
    /// `Err(Overflow)` leaves the stream position undefined; the
    /// runner quarantines and kills the child rather than resyncing.
    pub fn read_line(&mut self, max: usize) -> Result<String, LineError> {
        loop {
            if let Some(pos) = self.pending.iter().position(|b| *b == b'\n') {
                // The bound is on the *line* (§7.6): a newline that
                // arrives in the same chunk that crosses the limit is
                // still an overflow — the check is on the line length,
                // not on what happened to be buffered so far.
                if pos > max {
                    return Err(LineError::Overflow { scanned: pos, max });
                }
                let line: Vec<u8> = self.pending.drain(..=pos).collect();
                let mut line = line;
                line.pop(); // the newline itself
                return String::from_utf8(line)
                    .map_err(|_| LineError::Io("line is not UTF-8".to_string()));
            }
            if self.pending.len() > max {
                return Err(LineError::Overflow {
                    scanned: self.pending.len(),
                    max,
                });
            }
            let read = self
                .inner
                .read(&mut self.chunk)
                .map_err(|e| LineError::Io(e.to_string()))?;
            if read == 0 {
                return Err(LineError::Io("eof".to_string()));
            }
            self.pending.extend_from_slice(&self.chunk[..read]);
        }
    }

    /// Discards through the next `\n` (or EOF), memory-bounded — the
    /// stderr drainer's resync after an oversized line: bytes are
    /// dropped, never buffered (§10.5).
    pub fn discard_line(&mut self) -> bool {
        loop {
            if let Some(pos) = self.pending.iter().position(|b| *b == b'\n') {
                self.pending.drain(..=pos);
                return true;
            }
            self.pending.clear();
            match self.inner.read(&mut self.chunk) {
                Ok(0) => return false,
                Ok(read) => {
                    if let Some(pos) = self.chunk[..read].iter().position(|b| *b == b'\n') {
                        self.pending.extend_from_slice(&self.chunk[pos + 1..read]);
                        return true;
                    }
                }
                Err(_) => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_lines_and_keeps_remainder() {
        let mut reader = BoundedLineReader::new(&b"one\ntwo\nthree\n"[..]);
        assert_eq!(reader.read_line(64).unwrap(), "one");
        assert_eq!(reader.read_line(64).unwrap(), "two");
        assert_eq!(reader.read_line(64).unwrap(), "three");
        assert_eq!(
            reader.read_line(64).unwrap_err(),
            LineError::Io("eof".to_string())
        );
    }

    #[test]
    fn refuses_to_buffer_past_the_bound() {
        // A line over the bound, delivered in small chunks (a slow
        // hostile child): the refusal fires at the bound, not at EOF.
        let stream: &[u8] = &vec![b'x'; 5 * CHUNK];
        let mut reader = BoundedLineReader::new(stream);
        let err = reader.read_line(1024).unwrap_err();
        assert!(matches!(err, LineError::Overflow { scanned: _, max } if max == 1024));
    }

    #[test]
    fn a_line_exactly_at_the_bound_is_legal() {
        let payload = format!("{}\n", "x".repeat(1024));
        let mut reader = BoundedLineReader::new(payload.as_bytes());
        assert_eq!(reader.read_line(1024).unwrap().len(), 1024);
        // One over: overflow, even when the newline arrives in the
        // same chunk that crosses the limit.
        let payload = format!("{}\nnext\n", "x".repeat(1025));
        let mut reader = BoundedLineReader::new(payload.as_bytes());
        assert!(matches!(
            reader.read_line(1024).unwrap_err(),
            LineError::Overflow { .. }
        ));
    }

    #[test]
    fn write_line_checks_the_bound_before_writing() {
        let mut sink: Vec<u8> = Vec::new();
        assert!(write_line(&mut sink, "ok", 64).is_ok());
        assert_eq!(sink, b"ok\n");
        let long = "x".repeat(65);
        assert_eq!(
            write_line(&mut sink, &long, 64).unwrap_err(),
            LineError::Overflow {
                scanned: 65,
                max: 64
            }
        );
    }
}
