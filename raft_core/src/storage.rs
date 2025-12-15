use crate::log::LogEntry;
use crate::state::{NodeId, Term};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: Term,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Metadata {
    current_term: Term,
    voted_for: Option<NodeId>,
}

pub struct Storage {
    log_file: PathBuf,
    metadata_file: PathBuf,
    snapshot_file: PathBuf,
}

impl Storage {
    pub fn new(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("Failed to create data directory: {:?}", data_dir))?;

        let log_file = data_dir.join("log.bin");
        let metadata_file = data_dir.join("metadata.bin");
        let snapshot_file = data_dir.join("snapshot.bin");

        if !log_file.exists() {
            File::create(&log_file)?;
        }
        if !metadata_file.exists() {
            File::create(&metadata_file)?;
        }
        if !snapshot_file.exists() {
            File::create(&snapshot_file)?;
        }

        Ok(Self {
            log_file,
            metadata_file,
            snapshot_file,
        })
    }

    pub fn load_metadata(&self) -> Result<(Term, Option<NodeId>)> {
        let file = File::open(&self.metadata_file)?;
        if file.metadata()?.len() == 0 {
            return Ok((0, None));
        }
        let reader = BufReader::new(file);
        let metadata: Metadata =
            bincode::deserialize_from(reader).with_context(|| "Failed to deserialize metadata")?;
        Ok((metadata.current_term, metadata.voted_for))
    }

    pub fn save_metadata(&self, term: Term, voted_for: Option<NodeId>) -> Result<()> {
        let metadata = Metadata {
            current_term: term,
            voted_for,
        };
        let mut file = BufWriter::new(File::create(&self.metadata_file)?);
        bincode::serialize_into(&mut file, &metadata)
            .with_context(|| "Failed to serialize metadata")?;
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(())
    }

    // TODO: ig good enough but can refactor with using zero copy &[u8]
    pub fn load_log(&self) -> Result<Vec<LogEntry>> {
        let file = File::open(&self.log_file)?;
        if file.metadata()?.len() == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut shared_buffer: Vec<u8> = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(_) => {
                    // ok
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // expected eof
                    break;
                }
                Err(e) => {
                    // unexpected
                    return Err(e.into());
                }
            }
            let len = u32::from_le_bytes(len_bytes) as usize;
            shared_buffer.resize(len, 0);
            reader.read_exact(&mut shared_buffer)?;
            let entry: LogEntry = bincode::deserialize(&shared_buffer)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    pub fn append_log_entry(&self, entry: &LogEntry) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.log_file)?;
        let serialized = bincode::serialize(entry)?;
        let len = serialized.len() as u32;

        file.write_all(&len.to_le_bytes())?;
        file.write_all(&serialized)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn discard_log_after(&self, at_index: u64) -> Result<()> {
        let temp_log_file = self.log_file.with_extension("tmp");
        let entries = self.load_log()?;
        let mut temp_file = File::create(&temp_log_file)?;
        for entry in entries.iter().filter(|e| e.index < at_index) {
            let serialized = bincode::serialize(entry)?;
            let len = serialized.len() as u32;
            temp_file.write_all(&len.to_le_bytes())?;
            temp_file.write_all(&serialized)?;
        }
        temp_file.sync_all()?;
        std::fs::rename(&temp_log_file, &self.log_file)?;
        Ok(())
    }

    pub fn discard_log_before(&self, from_index: u64) -> Result<()> {
        let temp_log_file = self.log_file.with_extension("tmp");
        let entries = self.load_log()?;

        let mut temp_file = File::create(&temp_log_file)?;
        for entry in entries.iter().filter(|e| e.index >= from_index) {
            let serialized = bincode::serialize(entry)?;
            let len = serialized.len() as u32;
            temp_file.write_all(&len.to_le_bytes())?;
            temp_file.write_all(&serialized)?;
        }
        temp_file.sync_all()?;
        std::fs::rename(&temp_log_file, &self.log_file)?;
        Ok(())
    }

    pub fn save_snapshot(
        &self,
        metadata: &SnapshotMetadata,
        state_machine: &HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        let file = File::create(&self.snapshot_file)?;
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, metadata)?;
        bincode::serialize_into(&mut writer, state_machine)?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn load_snapshot(&self) -> Result<Option<(SnapshotMetadata, HashMap<String, Vec<u8>>)>> {
        let file = File::open(&self.snapshot_file)?;
        if file.metadata()?.len() == 0 {
            println!(
                "[STORAGE] Snapshot file is empty or does not exist: {:?}",
                self.snapshot_file
            );
            return Ok(None);
        }
        let mut reader = BufReader::new(file);
        let metadata: SnapshotMetadata = bincode::deserialize_from(&mut reader)?;
        let state_machine: HashMap<String, Vec<u8>> = bincode::deserialize_from(&mut reader)?;
        Ok(Some((metadata, state_machine)))
    }
}
