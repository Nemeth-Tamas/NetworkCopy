use std::io::{Read, Write};

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub rel_path: String,
    pub size: u64,
    pub modified: u64, // Last modification time in seconds since Unix Epoch
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

        files.push(FileEntry { rel_path, size, modified });
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
