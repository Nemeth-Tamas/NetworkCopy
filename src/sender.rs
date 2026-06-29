use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;

use crate::protocol::{self, FileEntry};

/// Recursively scas a directory in parallel using Rayon.
pub fn scan_directory(base_dir: &Path) -> std::io::Result<Vec<FileEntry>> {
    fn walk(dir: PathBuf, base_dir: PathBuf) -> std::io::Result<Vec<FileEntry>> {
        let mut entries = Vec::new();
        let read_dir = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                eprintln!("⚠️ Warning: Skipping directory {:?} (Read error: {})", dir, e);
                return Ok(entries);
            }
        };
        let mut subdirs = Vec::new();

        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("⚠️ Warning: Skipping directory entry in {:?} (Read error: {})", dir, e);
                    continue;
                }
            };
            let path = entry.path();
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("⚠️ Warning: Skipping entry metadata {:?} (Metadata error: {})", path, e);
                    continue;
                }
            };

            if meta.is_dir() {
                subdirs.push(path);
            } else if meta.is_file() {
                // Test openability to skip locked/inaccessible files early
                match std::fs::File::open(&path) {
                    Ok(_) => {
                        if let Ok(rel) = path.strip_prefix(&base_dir) {
                            if let Some(rel_str) = rel.to_str() {
                                let modified = meta.modified()
                                    .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                                        .unwrap_or(std::time::Duration::ZERO)
                                        .as_secs())
                                    .unwrap_or(0);
                                entries.push(FileEntry {
                                    // Standardize path separators to forward slash for cross-platform safety
                                    rel_path: rel_str.replace('\\', "/"),
                                    size: meta.len(),
                                    modified,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️ Warning: Skipping inaccessible file {:?} (Cannot open: {})", path, e);
                    }
                }
            }
        }

        if subdirs.is_empty() {
            Ok(entries)
        } else {
            // Scan subdirectories in parallel
            let sub_results: Result<Vec<Vec<FileEntry>>, std::io::Error> = subdirs
                .into_par_iter()
                .map(|subdir| walk(subdir, base_dir.clone()))
                .collect();

            let flat_results = sub_results?;
            for mut sub_res in flat_results {
                entries.append(&mut sub_res);
            }
            Ok(entries)
        }
    }

    walk(base_dir.to_path_buf(), base_dir.to_path_buf())
}

/// Runs a multi-stream network speedtest to find the optimal number of streams.
pub fn run_speedtest_client(control_stream: &mut TcpStream, receiver_addr: &str) -> std::io::Result<usize> {
    let test_configs = vec![1, 2, 4, 8];
    let mut best_streams = 4;
    let mut best_speed = 0.0;

    println!("\n⚡ Running network speedtest to determine optimal stream count...");

    for &k in &test_configs {
        // Send command '1' (run test configuration) and K (stream count)
        control_stream.write_all(&[1u8])?;
        control_stream.write_all(&(k as u32).to_be_bytes())?;

        // Wait for receiver to prepare
        let mut ready = [0u8; 1];
        control_stream.read_exact(&mut ready)?;

        let stop_signal = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(k);

        // Pre-connect all streams to prevent server-side hang
        let mut sockets = Vec::with_capacity(k);
        for stream_idx in 0..k {
            let mut socket = TcpStream::connect(receiver_addr)?;
            socket.write_all(b"FSTD")?;
            // Send special index with MSB set to indicate speedtest stream
            let speedtest_idx = (stream_idx as u32) | 0x8000_0000;
            socket.write_all(&speedtest_idx.to_be_bytes())?;
            sockets.push(socket);
        }

        // Spawn threads to flood the streams with dummy data
        for mut socket in sockets {
            let stop = Arc::clone(&stop_signal);
            let handle = thread::spawn(move || -> std::io::Result<()> {
                let buffer = [0u8; 64 * 1024]; // 64KB dummy buffer
                while !stop.load(Ordering::Relaxed) {
                    socket.write_all(&buffer)?;
                }
                Ok(())
            });
            handles.push(handle);
        }

        // Run the burst for 500ms
        thread::sleep(Duration::from_millis(500));
        stop_signal.store(true, Ordering::Relaxed);

        // Join threads to close the sockets (this triggers EOF on the server)
        for handle in handles {
            let _ = handle.join();
        }

        // Read calculated bandwidth from receiver
        let mut speed_bytes = [0u8; 8];
        control_stream.read_exact(&mut speed_bytes)?;
        let bytes_per_sec = u64::from_be_bytes(speed_bytes);
        let speed_mb = bytes_per_sec as f64 / 1_048_576.0;

        println!("   📊 {} Streams: {:.2} MB/s", k, speed_mb);
        if speed_mb > best_speed {
            best_speed = speed_mb;
            best_streams = k;
        }
    }

    // Send command '0' (speedtest finished)
    control_stream.write_all(&[0u8])?;

    println!("🏆 Selected optimal configuration: {} streams (max speed: {:.2} MB/s)\n", best_streams, best_speed);
    Ok(best_streams)
}

/// Runs a UDP client socket to scan for a broadcasting Receiver.
pub fn discover_receiver() -> std::io::Result<Option<String>> {
    println!("🔍 Scanning local network for Receiver (UDP discovery on port 7879)...");
    let socket = match std::net::UdpSocket::bind("0.0.0.0:7879") {
        Ok(s) => s,
        Err(e) => {
            println!("⚠️ Failed to bind UDP discovery socket: {}", e);
            return Ok(None);
        }
    };
    socket.set_read_timeout(Some(Duration::from_secs(3)))?;

    let mut buffer = [0u8; 1024];
    match socket.recv_from(&mut buffer) {
        Ok((amt, src_addr)) => {
            let msg = std::str::from_utf8(&buffer[..amt]).unwrap_or("");
            if msg.starts_with("FSTP-RECEIVER:") {
                let parts: Vec<&str> = msg.split(':').collect();
                if parts.len() == 2 {
                    let port = parts[1];
                    let ip = src_addr.ip().to_string();
                    return Ok(Some(format!("{}:{}", ip, port)));
                }
            }
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
            println!("⚠️ UDP Discovery timed out (no Receiver found).");
        }
        Err(e) => return Err(e),
    }
    Ok(None)
}

/// Runs the sender process
pub fn run_sender(
    src_dir: PathBuf,
    receiver_addr: &str,
    mut num_streams: usize,
    use_compression: bool,
) -> std::io::Result<()> {
    println!("🔍 Indexing source directory: {:?}", src_dir);
    let scan_start = Instant::now();
    let files = scan_directory(&src_dir)?;
    let scan_duration = scan_start.elapsed();

    let total_bytes: u64 = files.iter().map(|f| f.size).sum();
    println!(
        "✅ Indexed {} files ({:.2} MB) in {:?}",
        files.len(),
        total_bytes as f64 / 1_048_576.0,
        scan_duration
    );

    println!("🔌 Connecting to receiver at {}...", receiver_addr);
    let mut control_stream = TcpStream::connect(receiver_addr)?;

    // Send control connection magic bytes
    control_stream.write_all(b"FSTP")?;
    // Send mode (1 = Send session)
    control_stream.write_all(&[1u8])?;

    // If stream count is specified as 0, or if we want to auto-tune, run the speedtest
    if num_streams == 0 {
        num_streams = run_speedtest_client(&mut control_stream, receiver_addr)?;
    } else {
        // Let the server know we are skipping the speedtest
        control_stream.write_all(&[0u8])?;
        println!("🚀 Skipping speedtest, using user-specified {} streams...", num_streams);
    }

    // Send selected stream count for the actual file transfer phase
    control_stream.write_all(&(num_streams as u32).to_be_bytes())?;

    // Send compression selection (1 = LZ4 compression, 0 = raw stream)
    control_stream.write_all(&[if use_compression { 1u8 } else { 0u8 }])?;

    println!("📤 Sending file index...");
    protocol::write_index(&mut control_stream, &files)?;

    println!("⏳ Waiting for receiver to check files for smart resume...");
    let to_transfer_indices = protocol::read_transfer_list(&mut control_stream)?;

    let mut files_to_transfer = Vec::with_capacity(to_transfer_indices.len());
    for &idx in &to_transfer_indices {
        if (idx as usize) < files.len() {
            files_to_transfer.push(files[idx as usize].clone());
        }
    }
    let total_bytes_to_transfer: u64 = files_to_transfer.iter().map(|f| f.size).sum();
    println!(
        "📂 Files to transfer: {}/{} ({:.2} MB). Skipping the rest.",
        files_to_transfer.len(),
        files.len(),
        total_bytes_to_transfer as f64 / 1_048_576.0
    );

    println!("⏳ Waiting for receiver to prepare filesystem...");
    let mut confirmation = [0u8; 1];
    control_stream.read_exact(&mut confirmation)?;

    if confirmation[0] != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Receiver rejected transfer",
        ));
    }
    println!("🚀 Receiver is ready! Starting multi-stream transfer...");

    // Partition files into load-balanced buckets
    let buckets = protocol::partition_files(&files_to_transfer, num_streams);

    // Initialize progress bar
    let pb = ProgressBar::new(total_bytes_to_transfer);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({speed}, eta {eta})")
            .unwrap()
            .progress_chars("#>-")
    );
    let pb = Arc::new(pb);

    // Spawn N sender threads
    let mut handles = Vec::with_capacity(num_streams);
    let src_dir_arc = Arc::new(src_dir);
    let receiver_addr_str = receiver_addr.to_string();

    for (stream_idx, bucket) in buckets.into_iter().enumerate() {
        let pb = Arc::clone(&pb);
        let src_dir = Arc::clone(&src_dir_arc);
        let addr = receiver_addr_str.clone();

        let handle = thread::spawn(move || -> std::io::Result<()> {
            if bucket.is_empty() {
                return Ok(());
            }

            // Open a data connection to the receiver
            let mut data_stream = TcpStream::connect(&addr)?;
            data_stream.write_all(b"FSTD")?;
            data_stream.write_all(&(stream_idx as u32).to_be_bytes())?;

            // Wrap in compression if enabled
            let mut writer = if use_compression {
                protocol::StreamType::Compressed(lz4_flex::frame::FrameEncoder::new(data_stream))
            } else {
                protocol::StreamType::Raw(data_stream)
            };

            let mut buffer = [0u8; 64 * 1024]; // 64KB buffer

            for file_entry in bucket {
                if file_entry.size == 0 {
                    continue;
                }

                let full_path = src_dir.join(&file_entry.rel_path);
                let file = File::open(&full_path)?;
                let mut reader = BufReader::new(file);
                let mut bytes_left = file_entry.size;

                let mut hasher = crc32fast::Hasher::new();

                while bytes_left > 0 {
                    let to_read = std::cmp::min(bytes_left, buffer.len() as u64) as usize;
                    reader.read_exact(&mut buffer[..to_read])?;
                    writer.write_all(&buffer[..to_read])?;
                    hasher.update(&buffer[..to_read]);
                    pb.inc(to_read as u64);
                    bytes_left -= to_read as u64;
                }

                // Write 4-byte CRC32 checksum
                let checksum = hasher.finalize();
                writer.write_all(&checksum.to_be_bytes())?;
            }

            writer.flush()?;
            if let protocol::StreamType::Compressed(encoder) = writer {
                encoder.finish().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }
            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all data streams to finish
    let mut thread_errors = Vec::new();
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => thread_errors.push(format!("Stream {} error: {}", i, e)),
            Err(_) => thread_errors.push(format!("Stream {} panicked", i)),
        }
    }

    pb.finish_with_message("Transfer complete");

    if !thread_errors.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Transfer failed with errors: {:?}", thread_errors),
        ));
    }

    println!("🎉 All streams completed successfully!");
    Ok(())
}

