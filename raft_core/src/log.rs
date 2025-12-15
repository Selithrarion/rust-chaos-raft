use crate::state::Term;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: u64,
    pub command: Vec<u8>, // binary
}

impl fmt::Debug for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogEntry")
            .field("term", &self.term)
            .field("index", &self.index)
            .field("command", &String::from_utf8_lossy(&self.command))
            .finish()
    }
}
