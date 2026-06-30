use std::io::{Read, Write};
use std::path::{Component, Path};

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub rel_path: String,
    pub size: u64,
    pub modified: u64, // Last modification time in seconds since Unix Epoch
    pub crc32: u32,    // CRC32 checksum of the file
}

/// Sanitizes relative paths to prevent directory traversal and OS exploits.
pub fn sanitize_rel_path(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    // Reject absolute checks
    if path.starts_with('/') || path.starts_with('\\') {
        return None;
    }

    // Windows drive prefixes check (e.g. C:)
    if path.chars().next()?.is_alphabetic() && path.chars().nth(1) == Some(':') {
        return None;
    }

    let p = Path::new(path);
    let mut normalized = Vec::new();

    for comp in p.components() {
        match comp {
            Component::Normal(os_str) => {
                let comp_str = comp_str_to_safe_string(os_str.to_str()?)?;
                normalized.push(comp_str);
            }
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => {
                // Reject absolute prefixes and parent traversals
                return None;
            }
            Component::CurDir => {}
        }
    }

    if normalized.is_empty() {
        return None;
    }

    Some(normalized.join("/"))
}

fn comp_str_to_safe_string(comp_str: &str) -> Option<String> {
    // Reject trailing space or dot
    if comp_str.ends_with(' ') || comp_str.ends_with('.') {
        return None;
    }

    // Check Windows reserved device names
    let lower = comp_str.to_lowercase();
    let base_name = lower.split('.').next().unwrap_or("");
    match base_name {
        "con" | "prn" | "aux" | "nul" => return None,
        s if s.starts_with("com") && s.len() == 4 && s.chars().nth(3).map_or(false, |c| c.is_ascii_digit()) => return None,
        s if s.starts_with("lpt") && s.len() == 4 && s.chars().nth(3).map_or(false, |c| c.is_ascii_digit()) => return None,
        _ => {}
    }

    Some(comp_str.replace('\\', "/"))
}

/// Serializes the file index into the writer.
pub fn write_index<W: Write>(writer: &mut W, files: &[FileEntry]) -> std::io::Result<()> {
    // Write total number of files as u64 (big-endian)
    writer.write_all(&(files.len() as u64).to_be_bytes())?;

    for file in files {
        let path_bytes = file.rel_path.as_bytes();
        // Write path length as u32 (big-endian)
        writer.write_all(&(path_bytes.len() as u32).to_be_bytes())?;
        // Write path string bytes
        writer.write_all(path_bytes)?;
        // Write file size as u64 (big-endian)
        writer.write_all(&file.size.to_be_bytes())?;
        // Write modification time as u64 (big-endian)
        writer.write_all(&file.modified.to_be_bytes())?;
        // Write CRC32 checksum as u32 (big-endian)
        writer.write_all(&file.crc32.to_be_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

/// Deserializes the file index from the reader.
pub fn read_index<R: Read>(reader: &mut R) -> std::io::Result<Vec<FileEntry>> {
    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes)?;
    let count = u64::from_be_bytes(len_bytes);

    let mut files = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut path_len_bytes = [0u8; 4];
        reader.read_exact(&mut path_len_bytes)?;
        let path_len = u32::from_be_bytes(path_len_bytes) as usize;

        let mut path_bytes = vec![0u8; path_len];
        reader.read_exact(&mut path_bytes)?;
        let rel_path = String::from_utf8(path_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut size_bytes = [0u8; 8];
        reader.read_exact(&mut size_bytes)?;
        let size = u64::from_be_bytes(size_bytes);

        let mut modified_bytes = [0u8; 8];
        reader.read_exact(&mut modified_bytes)?;
        let modified = u64::from_be_bytes(modified_bytes);

        let mut crc_bytes = [0u8; 4];
        reader.read_exact(&mut crc_bytes)?;
        let crc32 = u32::from_be_bytes(crc_bytes);

        files.push(FileEntry { rel_path, size, modified, crc32 });
    }

    Ok(files)
}

/// Serializes the list of file indices to transfer.
pub fn write_transfer_list<W: Write>(writer: &mut W, indices: &[u32]) -> std::io::Result<()> {
    writer.write_all(&(indices.len() as u32).to_be_bytes())?;
    for &idx in indices {
        writer.write_all(&idx.to_be_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

/// Deserializes the list of file indices to transfer.
pub fn read_transfer_list<R: Read>(reader: &mut R) -> std::io::Result<Vec<u32>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let count = u32::from_be_bytes(len_bytes) as usize;

    let mut indices = Vec::with_capacity(count);
    for _ in 0..count {
        let mut idx_bytes = [0u8; 4];
        reader.read_exact(&mut idx_bytes)?;
        indices.push(u32::from_be_bytes(idx_bytes));
    }
    Ok(indices)
}

/// Partitions the list of files into `num_buckets` partitions using a greedy algorithm.
/// Sorts files by size descending, then assigns each file to the bucket with the smallest current total size.
/// Since the sorting and distribution are deterministic, both sender and receiver will produce the exact same partition.
pub fn partition_files(files: &[FileEntry], num_buckets: usize) -> Vec<Vec<FileEntry>> {
    if num_buckets == 0 {
        return Vec::new();
    }

    let mut buckets = vec![Vec::new(); num_buckets];
    let mut bucket_sizes = vec![0u64; num_buckets];

    // Sort a copy of files by size in descending order
    let mut sorted_files = files.to_vec();
    sorted_files.sort_by(|a, b| b.size.cmp(&a.size));

    for file in sorted_files {
        // Find the index of the bucket with the minimum total size
        let mut min_idx = 0;
        let mut min_size = bucket_sizes[0];
        for i in 1..num_buckets {
            if bucket_sizes[i] < min_size {
                min_idx = i;
                min_size = bucket_sizes[i];
            }
        }

        // Add file to this bucket
        bucket_sizes[min_idx] += file.size;
        buckets[min_idx].push(file);
    }

    buckets
}

pub enum StreamType<W: Write> {
    Raw(W),
    Compressed(lz4_flex::frame::FrameEncoder<W>),
}

impl<W: Write> Write for StreamType<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            StreamType::Raw(s) => s.write(buf),
            StreamType::Compressed(e) => e.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            StreamType::Raw(s) => s.flush(),
            StreamType::Compressed(e) => e.flush(),
        }
    }
}

pub enum StreamReader<R: Read> {
    Raw(R),
    Compressed(lz4_flex::frame::FrameDecoder<R>),
}

impl<R: Read> Read for StreamReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            StreamReader::Raw(s) => s.read(buf),
            StreamReader::Compressed(d) => d.read(buf),
        }
    }
}

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn compute_hmac(key: &str, data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

pub fn verify_hmac(key: &str, data: &[u8], signature: &[u8]) -> bool {
    if let Ok(mut mac) = HmacSha256::new_from_slice(key.as_bytes()) {
        mac.update(data);
        mac.verify_slice(signature).is_ok()
    } else {
        false
    }
}
