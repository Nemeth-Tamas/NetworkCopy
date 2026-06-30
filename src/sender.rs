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

#[cfg(unix)]
fn get_file_permissions(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode()
}

#[cfg(not(unix))]
fn get_file_permissions(_meta: &std::fs::Metadata) -> u32 {
    0o644
}

/// Recursively scas a directory in parallel using Rayon.
pub fn scan_directory(
    base_dir: &Path,
    includes: &[String],
    excludes: &[String],
    calculate_crc32: bool,
) -> std::io::Result<Vec<FileEntry>> {
    fn walk(
        dir: PathBuf,
        base_dir: PathBuf,
        includes: Arc<Vec<String>>,
        excludes: Arc<Vec<String>>,
        calculate_crc32: bool,
    ) -> std::io::Result<Vec<FileEntry>> {
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
                if let Ok(rel) = path.strip_prefix(&base_dir) {
                    if let Some(rel_str) = rel.to_str() {
                        let rel_path_normalized = rel_str.replace('\\', "/");
                        
                        // Apply include/exclude glob filters
                        if should_skip(&rel_path_normalized, &includes, &excludes) {
                            continue;
                        }

                        // Test openability to skip locked/inaccessible files early
                        match std::fs::File::open(&path) {
                            Ok(_) => {
                                let modified = meta.modified()
                                    .map(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH)
                                        .unwrap_or(std::time::Duration::ZERO)
                                        .as_secs())
                                    .unwrap_or(0);

                                let crc32 = if calculate_crc32 {
                                    match calculate_file_crc32(&path) {
                                        Ok(c) => c,
                                        Err(e) => {
                                            eprintln!("⚠️ Warning: Failed to compute CRC32 for {:?}: {}", path, e);
                                            continue; // Skip file if we can't read it to calculate checksum
                                        }
                                    }
                                } else {
                                    0
                                };

                                let permissions = get_file_permissions(&meta);

                                entries.push(FileEntry {
                                    rel_path: rel_path_normalized,
                                    size: meta.len(),
                                    modified,
                                    crc32,
                                    permissions,
                                    offset: 0,
                                });
                            }
                            Err(e) => {
                                eprintln!("⚠️ Warning: Skipping inaccessible file {:?} (Cannot open: {})", path, e);
                            }
                        }
                    }
                }
            }
        }

        if subdirs.is_empty() {
            Ok(entries)
        } else {
            // Scan subdirectories in parallel
            let includes_clone = Arc::clone(&includes);
            let excludes_clone = Arc::clone(&excludes);
            let sub_results: Result<Vec<Vec<FileEntry>>, std::io::Error> = subdirs
                .into_par_iter()
                .map(|subdir| walk(subdir, base_dir.clone(), Arc::clone(&includes_clone), Arc::clone(&excludes_clone), calculate_crc32))
                .collect();

            let flat_results = sub_results?;
            for mut sub_res in flat_results {
                entries.append(&mut sub_res);
            }
            Ok(entries)
        }
    }

    let includes_arc = Arc::new(includes.to_vec());
    let excludes_arc = Arc::new(excludes.to_vec());
    walk(base_dir.to_path_buf(), base_dir.to_path_buf(), includes_arc, excludes_arc, calculate_crc32)
}

fn should_skip(rel_path: &str, includes: &[String], excludes: &[String]) -> bool {
    // 1. Exclude checks
    for pattern in excludes {
        if let Ok(glob_pattern) = glob::Pattern::new(pattern) {
            if glob_pattern.matches(rel_path) || rel_path.split('/').any(|comp| glob_pattern.matches(comp)) {
                return true;
            }
        }
    }

    // 2. Include checks
    if !includes.is_empty() {
        let mut matched = false;
        for pattern in includes {
            if let Ok(glob_pattern) = glob::Pattern::new(pattern) {
                if glob_pattern.matches(rel_path) || rel_path.split('/').any(|comp| glob_pattern.matches(comp)) {
                    matched = true;
                    break;
                }
            }
        }
        if !matched {
            return true; // Skip if it doesn't match any include pattern
        }
    }

    false
}

fn calculate_file_crc32(path: &Path) -> std::io::Result<u32> {
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0u8; 64 * 1024];
    let mut hasher = crc32fast::Hasher::new();
    loop {
        let n = std::io::Read::read(&mut file, &mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.finalize())
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


#[derive(Debug, Clone, Default)]
pub struct SenderOptions {
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub verify_existing: bool,
    pub dry_run: bool,
    pub no_discovery: bool,
    pub auth_key: Option<String>,
    pub control_port: u16,
    pub discovery_port: u16,
    pub auto_accept: bool,
    pub pairing_code: Option<String>,
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 { return None; }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i+2], 16).ok())
        .collect()
}

/// Runs a UDP client socket to scan for a broadcasting Receiver.
pub fn discover_receiver(discovery_port: u16, auth_key: Option<String>) -> std::io::Result<Option<String>> {
    println!("🔍 Scanning local network for Receiver (UDP discovery on port {})...", discovery_port);
    let socket = match std::net::UdpSocket::bind(format!("0.0.0.0:{}", discovery_port)) {
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
                if parts.len() == 3 {
                    if auth_key.is_some() {
                        println!("⚠️ Discovered receiver does not use authentication, but authentication is required locally!");
                        return Ok(None);
                    }
                    let port = parts[1];
                    let ip = src_addr.ip().to_string();
                    return Ok(Some(format!("{}:{}", ip, port)));
                } else if parts.len() == 5 {
                    let port = parts[1];
                    let hostname = parts[2];
                    let timestamp_str = parts[3];
                    let sig_hex = parts[4];

                    if let Some(key) = auth_key {
                        let timestamp = timestamp_str.parse::<u64>().unwrap_or(0);
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        // Prevent replay attack: check if timestamp is within 30 seconds
                        if now.saturating_sub(timestamp) > 30 && timestamp.saturating_sub(now) > 30 {
                            println!("⚠️ UDP Discovery authentication signature expired!");
                            return Ok(None);
                        }

                        let msg_prefix = format!("FSTP-RECEIVER:{}:{}:{}", port, hostname, timestamp_str);
                        if let Some(sig_bytes) = hex_decode(sig_hex) {
                            if protocol::verify_hmac(&key, msg_prefix.as_bytes(), &sig_bytes) {
                                let ip = src_addr.ip().to_string();
                                return Ok(Some(format!("{}:{}", ip, port)));
                            } else {
                                println!("⚠️ UDP Discovery authentication signature invalid!");
                            }
                        }
                    } else {
                        println!("⚠️ Discovered receiver uses authentication, but local machine has no auth key configured!");
                    }
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
    options: SenderOptions,
) -> std::io::Result<()> {
    println!("🔍 Indexing source directory: {:?}", src_dir);
    let scan_start = Instant::now();
    let files = scan_directory(&src_dir, &options.includes, &options.excludes, options.verify_existing)?;
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
    // Send protocol version (2 = v2.0)
    control_stream.write_all(&[2u8])?;

    // --- AUTHENTICATION HANDSHAKE ---
    let mut auth_required_byte = [0u8; 1];
    control_stream.read_exact(&mut auth_required_byte)?;
    let auth_required = auth_required_byte[0] == 1;

    if auth_required {
        if let Some(key) = &options.auth_key {
            // Read 32-byte challenge
            let mut challenge = [0u8; 32];
            control_stream.read_exact(&mut challenge)?;

            // Compute HMAC response
            let response = protocol::compute_hmac(key, &challenge);
            control_stream.write_all(&response)?;

            // Read validation result
            let mut result = [0u8; 1];
            control_stream.read_exact(&mut result)?;
            if result[0] != 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "HMAC authentication failed (Receiver rejected connection)",
                ));
            }
            println!("🔒 HMAC authentication successful!");
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Receiver requires authentication, but no local `--auth` key was provided!",
            ));
        }
    } else if options.auth_key.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Local machine expects authentication, but the Receiver is running in unauthenticated mode!",
        ));
    }

    // --- PAIRING CODE HANDSHAKE ---
    let mut pairing_required_byte = [0u8; 1];
    control_stream.read_exact(&mut pairing_required_byte)?;
    let pairing_required = pairing_required_byte[0] == 1;

    if pairing_required {
        let code = if let Some(c) = &options.pairing_code {
            c.clone()
        } else if options.auto_accept {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Pairing code confirmation is required, but `--yes` / automatic confirmation is active without manual pairing input!",
            ));
        } else {
            print!("🔑 Enter pairing code displayed on receiver: ");
            std::io::stdout().flush().unwrap();
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        };

        // Write pairing code length and value
        let code_bytes = code.as_bytes();
        control_stream.write_all(&(code_bytes.len() as u32).to_be_bytes())?;
        control_stream.write_all(code_bytes)?;

        // Read pairing check result
        let mut pairing_result = [0u8; 1];
        control_stream.read_exact(&mut pairing_result)?;
        if pairing_result[0] != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Pairing verification failed (Incorrect pairing code)",
            ));
        }
        println!("🔑 Pairing verification successful!");
    }

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

    // Send dry-run flag
    control_stream.write_all(&[if options.dry_run { 1u8 } else { 0u8 }])?;

    println!("📤 Sending file index...");
    protocol::write_index(&mut control_stream, &files)?;

    println!("⏳ Waiting for receiver to check files for smart resume...");
    let to_transfer_requests = protocol::read_transfer_list(&mut control_stream)?;

    let mut files_to_transfer = Vec::with_capacity(to_transfer_requests.len());
    for req in &to_transfer_requests {
        if (req.file_idx as usize) < files.len() {
            let mut file_info = files[req.file_idx as usize].clone();
            file_info.offset = req.offset;
            files_to_transfer.push(file_info);
        }
    }
    let total_bytes_to_transfer: u64 = files_to_transfer.iter().map(|f| f.transfer_size()).sum();

    if options.dry_run {
        println!("\n================ [ DRY RUN REPORT ] ================");
        println!("📂 Files Scanned: {}", files.len());
        println!("⏭️ Files Skipped: {}", files.len() - files_to_transfer.len());
        println!("📥 Files to Copy: {}", files_to_transfer.len());
        println!("📊 Total Bytes to Copy: {:.2} MB", total_bytes_to_transfer as f64 / 1_048_576.0);
        println!("🧵 Parallel Streams Configured: {}", num_streams);

        let buckets = protocol::partition_files(&files_to_transfer, num_streams);
        println!("\n🧵 Estimated Bucket Splits (Longest Processing Time First):");
        for (i, bucket) in buckets.iter().enumerate() {
            let size: u64 = bucket.iter().map(|f| f.transfer_size()).sum();
            println!("  Stream {}: {} files ({:.2} MB)", i, bucket.len(), size as f64 / 1_048_576.0);
        }
        println!("====================================================");
        println!("🛑 Dry run complete. Connection terminated.");
        return Ok(());
    }

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

    let transfer_start = Instant::now();

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
                if file_entry.transfer_size() == 0 {
                    continue;
                }

                let full_path = src_dir.join(&file_entry.rel_path);
                let mut file = File::open(&full_path)?;
                if file_entry.offset > 0 {
                    use std::io::Seek;
                    file.seek(std::io::SeekFrom::Start(file_entry.offset))?;
                }
                let mut reader = BufReader::new(file);
                let mut bytes_left = file_entry.transfer_size();

                let mut hasher = crc32fast::Hasher::new();

                while bytes_left > 0 {
                    let to_read = std::cmp::min(bytes_left, buffer.len() as u64) as usize;
                    reader.read_exact(&mut buffer[..to_read])?;
                    writer.write_all(&buffer[..to_read])?;
                    hasher.update(&buffer[..to_read]);
                    pb.inc(to_read as u64);
                    bytes_left -= to_read as u64;
                }

                // Write 4-byte CRC32 checksum (of the transferred chunk only)
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

    let elapsed = transfer_start.elapsed();
    let avg_speed_mb = if elapsed.as_secs_f64() > 0.0 {
        (total_bytes_to_transfer as f64 / 1_048_576.0) / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("\n================ [ TRANSFER SUMMARY ] ================");
    println!("📂 Files Scanned: {}", files.len());
    println!("⏭️ Files Skipped: {}", files.len() - files_to_transfer.len());
    println!("📥 Files Transferred: {}", files_to_transfer.len());
    println!("📊 Total Data Transferred: {:.2} MB", total_bytes_to_transfer as f64 / 1_048_576.0);
    println!("⏱️ Time Elapsed: {:?}", elapsed);
    println!("⚡ Average Speed: {:.2} MB/s", avg_speed_mb);
    println!("🗜️ LZ4 Compression: {}", if use_compression { "ON" } else { "OFF" });
    println!("🧵 Parallel Streams: {}", num_streams);
    println!("======================================================\n");

    println!("🎉 All streams completed successfully!");
    Ok(())
}

pub fn run_benchmark_sender(
    receiver_addr: &str,
    num_streams: usize,
    duration_secs: u32,
    options: SenderOptions,
) -> std::io::Result<()> {
    println!("🔌 Connecting to receiver at {} for benchmark...", receiver_addr);
    let mut control_stream = TcpStream::connect(receiver_addr)?;

    // Send control connection magic bytes
    control_stream.write_all(b"FSTP")?;
    // Send mode (3 = Benchmark session)
    control_stream.write_all(&[3u8])?;
    // Send protocol version (2 = v2.0)
    control_stream.write_all(&[2u8])?;

    // --- AUTHENTICATION HANDSHAKE ---
    let mut auth_required_byte = [0u8; 1];
    control_stream.read_exact(&mut auth_required_byte)?;
    let auth_required = auth_required_byte[0] == 1;

    if auth_required {
        if let Some(key) = &options.auth_key {
            // Read 32-byte challenge
            let mut challenge = [0u8; 32];
            control_stream.read_exact(&mut challenge)?;

            // Compute HMAC response
            let response = protocol::compute_hmac(key, &challenge);
            control_stream.write_all(&response)?;

            // Read validation result
            let mut result = [0u8; 1];
            control_stream.read_exact(&mut result)?;
            if result[0] != 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "HMAC authentication failed (Receiver rejected connection)",
                ));
            }
            println!("🔒 HMAC authentication successful!");
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Receiver requires authentication, but no local `--auth` key was provided!",
            ));
        }
    } else if options.auth_key.is_some() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Local machine expects authentication, but the Receiver is running in unauthenticated mode!",
        ));
    }

    // --- PAIRING CODE HANDSHAKE ---
    let mut pairing_required_byte = [0u8; 1];
    control_stream.read_exact(&mut pairing_required_byte)?;
    let pairing_required = pairing_required_byte[0] == 1;

    if pairing_required {
        let code = if let Some(c) = &options.pairing_code {
            c.clone()
        } else if options.auto_accept {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Pairing code confirmation is required, but `--yes` / automatic confirmation is active without manual pairing input!",
            ));
        } else {
            print!("🔑 Enter pairing code displayed on receiver: ");
            std::io::stdout().flush().unwrap();
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        };

        // Write pairing code length and value
        let code_bytes = code.as_bytes();
        control_stream.write_all(&(code_bytes.len() as u32).to_be_bytes())?;
        control_stream.write_all(code_bytes)?;

        // Read pairing check result
        let mut pairing_result = [0u8; 1];
        control_stream.read_exact(&mut pairing_result)?;
        if pairing_result[0] != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Pairing verification failed (Incorrect pairing code)",
            ));
        }
        println!("🔑 Pairing verification successful!");
    }

    // Now call the benchmark client flood logic
    crate::benchmark::run_benchmark_client(receiver_addr, num_streams, duration_secs, control_stream)
}

