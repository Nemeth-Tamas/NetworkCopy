use crate::protocol;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use crate::encrypted_stream::{MaybeEncryptedStream, EncryptedStream};

/// Runs the server-side speedtest loop, listening and measuring client data bursts.
pub fn run_speedtest_server(
    control_stream: &mut MaybeEncryptedStream<TcpStream>,
    listener: &TcpListener,
    use_encryption: bool,
    session_key: [u8; 32],
) -> std::io::Result<()> {
    loop {
        let mut cmd = [0u8; 1];
        control_stream.read_exact(&mut cmd)?;

        if cmd[0] == 0 {
            // Speedtest finished/skipped
            break;
        }

        let mut stream_count_bytes = [0u8; 4];
        control_stream.read_exact(&mut stream_count_bytes)?;
        let k = u32::from_be_bytes(stream_count_bytes) as usize;

        // Send ready confirmation to client
        control_stream.write_all(&[1u8])?;
        control_stream.flush()?;

        // Pre-accept all data streams
        let mut sockets: Vec<MaybeEncryptedStream<TcpStream>> = Vec::with_capacity(k);
        for _ in 0..k {
            let (mut raw_socket, _) = listener.accept()?;

            // Verify data stream magic bytes
            let mut magic = [0u8; 4];
            raw_socket.read_exact(&mut magic)?;
            if &magic != b"FSTD" {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid speedtest stream magic",
                ));
            }

            // Verify stream index (has speedtest bit set)
            let mut idx_bytes = [0u8; 4];
            raw_socket.read_exact(&mut idx_bytes)?;
            let idx = u32::from_be_bytes(idx_bytes);
            if idx & 0x8000_0000 == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected speedtest stream bit",
                ));
            }

            let socket = if use_encryption {
                MaybeEncryptedStream::Encrypted(EncryptedStream::new(raw_socket, session_key, idx))
            } else {
                MaybeEncryptedStream::Raw(raw_socket)
            };
            sockets.push(socket);
        }

        let bytes_received = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::with_capacity(k);
        let start_time = Instant::now();

        // Spawn discard threads
        for mut socket in sockets {
            let bytes_counter = Arc::clone(&bytes_received);
            let handle = thread::spawn(move || -> std::io::Result<()> {
                let mut buffer = [0u8; 64 * 1024]; // 64KB static buffer
                loop {
                    match socket.read(&mut buffer) {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            bytes_counter.fetch_add(n as u64, Ordering::Relaxed);
                        }
                        Err(_) => break,
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }

        // Wait for all burst streams to close (when client terminates them)
        for handle in handles {
            let _ = handle.join();
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        let total_bytes = bytes_received.load(Ordering::Relaxed);
        let bytes_per_sec = if elapsed > 0.0 {
            (total_bytes as f64 / elapsed) as u64
        } else {
            0
        };

        // Send measured speed back to client
        control_stream.write_all(&bytes_per_sec.to_be_bytes())?;
        control_stream.flush()?;
    }

    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct ReceiverOptions {
    pub verify_existing: bool,
    pub loop_mode: bool,
    pub auth_key: Option<String>,
    pub control_port: u16,
    pub discovery_port: u16,
    pub pairing_code: Option<String>,
    pub encrypt: bool,
}


fn get_hostname() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "Unknown".to_string())
}

pub fn generate_challenge() -> [u8; 32] {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let val = COUNTER.fetch_add(1, Ordering::SeqCst);
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(&val.to_be_bytes());
    hasher.update(&std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos().to_be_bytes());
    hasher.finalize().into()
}

pub fn generate_pairing_code() -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos();
    let code = (nanos % 9000) + 1000;
    code.to_string()
}

fn calculate_file_crc32(path: &Path) -> std::io::Result<u32> {
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 64 * 1024];
    let mut hasher = crc32fast::Hasher::new();
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.finalize())
}

/// Spawns a background thread broadcasting FSTP-RECEIVER presence over UDP.
pub fn start_udp_broadcaster(
    listen_port: u16,
    discovery_port: u16,
    auth_key: Option<String>,
    stop_flag: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.set_broadcast(true)?;

    thread::spawn(move || {
        let hostname = get_hostname();
        let broadcast_addr = format!("255.255.255.255:{}", discovery_port);
        
        while !stop_flag.load(Ordering::Relaxed) {
            let message = if let Some(key) = &auth_key {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let prefix = format!("FSTP-RECEIVER:{}:{}:{}", listen_port, hostname, timestamp);
                let sig = protocol::compute_hmac(key, prefix.as_bytes());
                let sig_hex: String = sig.iter().map(|b| format!("{:02x}", b)).collect();
                format!("{}:{}", prefix, sig_hex)
            } else {
                format!("FSTP-RECEIVER:{}:{}", listen_port, hostname)
            };
            
            let _ = socket.send_to(message.as_bytes(), &broadcast_addr);
            thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    Ok(())
}

pub fn run_receiver(
    mut dst_dir: PathBuf,
    listen_addr: &str,
    is_interactive: bool,
    options: ReceiverOptions,
) -> std::io::Result<()> {
    // Start UDP broadcaster in background
    let stop_broadcaster = Arc::new(AtomicBool::new(false));
    let stop_broadcaster_clone = Arc::clone(&stop_broadcaster);
    // Parse port from listen_addr (default 7878 if parsing fails)
    let port = listen_addr.split(':').last().and_then(|p| p.parse::<u16>().ok()).unwrap_or(7878);
    start_udp_broadcaster(port, options.discovery_port, options.auth_key.clone(), stop_broadcaster_clone)?;

    println!("👂 Listening for connection on {}...", listen_addr);
    let listener = TcpListener::bind(listen_addr)?;

    loop {
        // Accept control connection
        let (mut control_stream, client_addr) = listener.accept()?;
        println!("🔌 Connection established with client: {}", client_addr);

        // Read control magic bytes
        let mut magic = [0u8; 4];
        if let Err(_) = control_stream.read_exact(&mut magic) {
            if !options.loop_mode { break; }
            continue; // peer disconnected
        }
        if &magic != b"FSTP" {
            eprintln!("⚠️ Invalid control stream magic bytes");
            if !options.loop_mode {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid magic bytes"));
            }
            continue;
        }

        // Read mode
        let mut mode = [0u8; 1];
        control_stream.read_exact(&mut mode)?;
        if mode[0] != 1 && mode[0] != 3 {
            eprintln!("⚠️ Invalid transfer mode");
            if !options.loop_mode {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid transfer mode"));
            }
            continue;
        }

        // Read protocol version (2 = v2.0)
        let mut version = [0u8; 1];
        control_stream.read_exact(&mut version)?;
        if version[0] != 2 {
            eprintln!("⚠️ Protocol version mismatch: expected 2, got {}", version[0]);
            if !options.loop_mode {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Protocol version mismatch"));
            }
            continue;
        }

        // Read encrypt flag
        let mut encrypt_flag = [0u8; 1];
        control_stream.read_exact(&mut encrypt_flag)?;
        let client_wants_encrypt = encrypt_flag[0] == 1;

        if client_wants_encrypt != options.encrypt {
            eprintln!("⚠️ Encryption settings mismatch: client wants encrypt = {}, server config = {}", client_wants_encrypt, options.encrypt);
            if !options.loop_mode {
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Encryption config mismatch"));
            }
            continue;
        }

        let mut session_key = [0u8; 32];
        if options.encrypt {
            let mut sender_nonce = [0u8; 32];
            control_stream.read_exact(&mut sender_nonce)?;

            use rand::RngCore;
            let mut receiver_nonce = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut receiver_nonce);
            control_stream.write_all(&receiver_nonce)?;
            control_stream.flush()?;

            use hmac::Mac;
            let mut mac = hmac::SimpleHmac::<sha2::Sha256>::new_from_slice(options.auth_key.as_ref().unwrap().as_bytes()).unwrap();
            mac.update(&sender_nonce);
            mac.update(&receiver_nonce);
            session_key = mac.finalize().into_bytes().into();
        }

        use crate::encrypted_stream::{MaybeEncryptedStream, EncryptedStream};
        let mut control_stream = if options.encrypt {
            MaybeEncryptedStream::Encrypted(EncryptedStream::new(control_stream, session_key, 0))
        } else {
            MaybeEncryptedStream::Raw(control_stream)
        };

        // --- AUTHENTICATION HANDSHAKE ---
        let auth_required = options.auth_key.is_some();
        control_stream.write_all(&[if auth_required { 1u8 } else { 0u8 }])?;
        control_stream.flush()?;

        if auth_required {
            let key = options.auth_key.as_ref().unwrap();
            let challenge = generate_challenge();
            control_stream.write_all(&challenge)?;
            control_stream.flush()?;

            // Read 32-byte response
            let mut response = [0u8; 32];
            if let Err(_) = control_stream.read_exact(&mut response) {
                if !options.loop_mode { break; }
                continue;
            }

            // Verify response
            if protocol::verify_hmac(key, &challenge, &response) {
                control_stream.write_all(&[1u8])?;
                control_stream.flush()?;
                println!("🔒 HMAC authentication successful!");
            } else {
                control_stream.write_all(&[0u8])?;
                control_stream.flush()?;
                eprintln!("❌ HMAC authentication failed!");
                if !options.loop_mode {
                    return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "HMAC verification failed"));
                }
                continue;
            }
        }

        // --- PAIRING CODE HANDSHAKE ---
        // Pairing is active in interactive mode when HMAC auth is NOT enabled
        let pairing_required = is_interactive && !auth_required;
        control_stream.write_all(&[if pairing_required { 1u8 } else { 0u8 }])?;
        control_stream.flush()?;

        if pairing_required {
            let pairing_code = if let Some(code) = &options.pairing_code {
                code.clone()
            } else {
                generate_pairing_code()
            };
            println!("\n========================================");
            println!("🔑 Pairing Code: {}", pairing_code);
            println!("========================================\n");

            // Read pairing code from client
            let mut len_bytes = [0u8; 4];
            if let Err(_) = control_stream.read_exact(&mut len_bytes) {
                if !options.loop_mode { break; }
                continue;
            }
            let len = u32::from_be_bytes(len_bytes) as usize;
            if len > 100 {
                if !options.loop_mode {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid pairing code length"));
                }
                continue;
            }
            let mut code_bytes = vec![0u8; len];
            if let Err(_) = control_stream.read_exact(&mut code_bytes) {
                if !options.loop_mode { break; }
                continue;
            }
            let received_code = String::from_utf8_lossy(&code_bytes).trim().to_string();

            if received_code == pairing_code {
                control_stream.write_all(&[1u8])?;
                control_stream.flush()?;
                println!("🔑 Pairing verification successful!");
            } else {
                control_stream.write_all(&[0u8])?;
                control_stream.flush()?;
                eprintln!("❌ Pairing verification failed!");
                if !options.loop_mode {
                    return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Pairing verification failed"));
                }
                continue;
            }
        }

        if mode[0] == 3 {
            if let Err(e) = crate::benchmark::run_speedtest_benchmark_server(&mut control_stream, &listener, options.encrypt, session_key) {
                eprintln!("⚠️ Benchmark failed: {}", e);
            }
            if !options.loop_mode {
                break;
            }
            continue;
        }

        // Run speedtest server logic (it will loop until command 0 is received)
        if let Err(e) = run_speedtest_server(&mut control_stream, &listener, options.encrypt, session_key) {
            eprintln!("⚠️ Speedtest failed: {}", e);
            if !options.loop_mode {
                return Err(e);
            }
            continue;
        }

        // Read actual stream count for data transfer
        let mut stream_count_bytes = [0u8; 4];
        control_stream.read_exact(&mut stream_count_bytes)?;
        let num_streams = u32::from_be_bytes(stream_count_bytes) as usize;

        // Read compression flag (1 = enabled, 0 = disabled)
        let mut comp_byte = [0u8; 1];
        control_stream.read_exact(&mut comp_byte)?;
        let use_compression = comp_byte[0] == 1;
        if use_compression {
            println!("🗜️ LZ4 compression is enabled for this session.");
        } else {
            println!("🚀 Compression is disabled (raw transfer mode).");
        }

        // Read dry-run flag
        let mut dry_run_byte = [0u8; 1];
        control_stream.read_exact(&mut dry_run_byte)?;
        let is_dry_run = dry_run_byte[0] == 1;

        println!("📥 Receiving file index...");
        let mut files = protocol::read_index(&mut control_stream)?;

        // Sanitize and validate paths to prevent directory traversal and OS exploits
        let mut paths_ok = true;
        for file in &mut files {
            if let Some(safe_path) = protocol::sanitize_rel_path(&file.rel_path) {
                file.rel_path = safe_path;
            } else {
                eprintln!("❌ Unsafe path detected and rejected: {:?}", file.rel_path);
                paths_ok = false;
                break;
            }
        }
        if !paths_ok {
            if !options.loop_mode {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unsafe paths detected"));
            }
            continue;
        }

        // Smart Resume: Check existing files and negotiate resume offsets
        println!("🔍 Checking destination for files to skip (smart resume)...");
        let mut to_transfer_requests = Vec::new();
        for (idx, file) in files.iter_mut().enumerate() {
            let full_path = dst_dir.join(&file.rel_path);
            let mut skip = false;
            let mut resume_offset = 0u64;

            if full_path.exists() {
                if let Ok(meta) = std::fs::metadata(&full_path) {
                    if meta.is_file() && meta.len() == file.size {
                        if options.verify_existing {
                            // Compute local CRC32
                            if let Ok(local_crc) = calculate_file_crc32(&full_path) {
                                if local_crc == file.crc32 {
                                    skip = true;
                                }
                            }
                        } else {
                            if let Ok(modified_time) = meta.modified() {
                                if let Ok(duration) = modified_time.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                                    if duration.as_secs() == file.modified {
                                        skip = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !skip {
                // Determine if a partial temp file exists for resume
                let mut tmp_name = full_path.file_name().unwrap_or_default().to_os_string();
                tmp_name.push(".networkcopy-tmp");
                let tmp_path = full_path.with_file_name(tmp_name);

                if tmp_path.exists() {
                    if let Ok(tmp_meta) = std::fs::metadata(&tmp_path) {
                        let tmp_size = tmp_meta.len();
                        if tmp_size > 0 && tmp_size < file.size {
                            resume_offset = tmp_size;
                            file.offset = tmp_size; // Store in memory
                            println!("⏩ Found partial temp file for {:?}. Resuming from byte {}.", file.rel_path, resume_offset);
                        }
                    }
                }

                to_transfer_requests.push(protocol::TransferRequest {
                    file_idx: idx as u32,
                    offset: resume_offset,
                });
            }
        }

        // Send transfer list back to Sender
        protocol::write_transfer_list(&mut control_stream, &to_transfer_requests)?;

        if is_dry_run {
            println!("🛑 Dry-run requested by sender. Cleaning up session.");
            if !options.loop_mode {
                break;
            }
            continue;
        }

        // Filter index list
        let mut files_to_transfer = Vec::with_capacity(to_transfer_requests.len());
        for req in &to_transfer_requests {
            let mut file_info = files[req.file_idx as usize].clone();
            file_info.offset = req.offset;
            files_to_transfer.push(file_info);
        }

        let total_bytes_to_transfer: u64 = files_to_transfer.iter().map(|f| f.transfer_size()).sum();
        println!(
            "📂 Index processed: {}/{} files require transfer ({:.2} MB). Skipping others.",
            files_to_transfer.len(),
            files.len(),
            total_bytes_to_transfer as f64 / 1_048_576.0
        );

        println!("📁 Pre-creating directory structure at {:?}...", dst_dir);
        // Pre-create directories and empty files from the transfer list
        for file in &files_to_transfer {
            let full_path = dst_dir.join(&file.rel_path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Clean up pre-existing temp file from any aborted run, unless we are resuming it
            if file.size > 0 && file.offset == 0 {
                let mut tmp_name = full_path.file_name().unwrap_or_default().to_os_string();
                tmp_name.push(".networkcopy-tmp");
                let tmp_path = full_path.with_file_name(tmp_name);
                if tmp_path.exists() {
                    let _ = std::fs::remove_file(&tmp_path);
                }
            }

            // Pre-create zero-byte files now so we don't have to handle them in the stream loop
            if file.size == 0 {
                File::create(&full_path)?;
                // Set modification time on empty files too
                if let Some(modified_time) = std::time::SystemTime::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(file.modified)) {
                    let ft = filetime::FileTime::from_system_time(modified_time);
                    let _ = filetime::set_file_times(&full_path, ft, ft);
                }
            }
        }

        // Partition only the files we need to transfer
        let buckets = protocol::partition_files(&files_to_transfer, num_streams);

        // Send confirmation byte to Sender
        control_stream.write_all(&[1u8])?;
        control_stream.flush()?;

        println!("⏳ Waiting for {} parallel data streams...", num_streams);

        // Initialize progress bar
        let pb = ProgressBar::new(total_bytes_to_transfer);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({speed}, eta {eta})")
                .unwrap()
                .progress_chars("#>-")
        );
        let pb = Arc::new(pb);

        // Receive data connections and launch threads
        let dst_dir_arc = Arc::new(dst_dir.clone());
        let buckets_arc = Arc::new(buckets);
        let active_streams = buckets_arc.iter().filter(|b| !b.is_empty()).count();
        let mut handles = Vec::with_capacity(active_streams);

        for _ in 0..active_streams {
            let (mut socket, _) = listener.accept()?;

            // Verify data stream magic bytes
            let mut dmagic = [0u8; 4];
            socket.read_exact(&mut dmagic)?;
            if &dmagic != b"FSTD" {
                stop_broadcaster.store(true, Ordering::Relaxed);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid data stream magic bytes",
                ));
            }

            // Read stream index
            let mut idx_bytes = [0u8; 4];
            socket.read_exact(&mut idx_bytes)?;
            let stream_idx = u32::from_be_bytes(idx_bytes) as usize;

            if stream_idx >= num_streams {
                stop_broadcaster.store(true, Ordering::Relaxed);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Data stream index out of bounds",
                ));
            }

            let dst_dir = Arc::clone(&dst_dir_arc);
            let bucket = buckets_arc[stream_idx].clone();
            let pb = Arc::clone(&pb);
            let use_encryption = options.encrypt;
            let s_key = session_key;

            let socket = if use_encryption {
                MaybeEncryptedStream::Encrypted(EncryptedStream::new(socket, s_key, (stream_idx as u32) + 1))
            } else {
                MaybeEncryptedStream::Raw(socket)
            };

            let handle = thread::spawn(move || -> std::io::Result<()> {
                let mut buffer = [0u8; 64 * 1024]; // 64KB buffer

                // Wrap in decompression decoder if enabled
                let mut reader = if use_compression {
                    protocol::StreamReader::Compressed(lz4_flex::frame::FrameDecoder::new(socket))
                } else {
                    protocol::StreamReader::Raw(socket)
                };

                for file_entry in bucket {
                    if file_entry.transfer_size() == 0 {
                        continue;
                    }

                    let full_path = dst_dir.join(&file_entry.rel_path);
                    let mut tmp_name = full_path.file_name().unwrap_or_default().to_os_string();
                    tmp_name.push(".networkcopy-tmp");
                    let tmp_path = full_path.with_file_name(tmp_name);
                    
                    let file = if file_entry.offset > 0 {
                        std::fs::OpenOptions::new().write(true).append(true).open(&tmp_path)?
                    } else {
                        File::create(&tmp_path)?
                    };
                    let mut writer = BufWriter::new(file);
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
                    writer.flush()?;
                    drop(writer); // Close file handle

                    // Read and verify 4-byte CRC32 checksum (of the transferred chunk only)
                    let mut checksum_bytes = [0u8; 4];
                    reader.read_exact(&mut checksum_bytes)?;
                    let expected_checksum = u32::from_be_bytes(checksum_bytes);
                    let actual_checksum = hasher.finalize();

                    if expected_checksum != actual_checksum {
                        let _ = std::fs::remove_file(&tmp_path);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("CRC32 checksum mismatch for file: {:?}", file_entry.rel_path),
                        ));
                    }

                    // Rename temp file to target path
                    std::fs::rename(&tmp_path, &full_path)?;

                    // Preserve Unix permissions if applicable
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        let perms = std::fs::Permissions::from_mode(file_entry.permissions);
                        let _ = std::fs::set_permissions(&full_path, perms);
                    }

                    // Preserve modification times
                    if let Some(modified_time) = std::time::SystemTime::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(file_entry.modified)) {
                        let ft = filetime::FileTime::from_system_time(modified_time);
                        let _ = filetime::set_file_times(&full_path, ft, ft);
                    }
                }
                Ok(())
            });

            handles.push(handle);
        }

        // Wait for all receiver threads to complete
        let mut thread_errors = Vec::new();
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => thread_errors.push(format!("Receiver stream {} error: {}", i, e)),
                Err(_) => thread_errors.push(format!("Receiver stream {} panicked", i)),
            }
        }

        pb.finish_with_message("Transfer complete");

        if !thread_errors.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Transfer failed with errors: {:?}", thread_errors),
            ));
        }

        println!("🎉 All streams received and files written successfully!");

        if !options.loop_mode {
            break;
        }

        println!("\n🔄 Loop mode: waiting for next transfer...");
        if is_interactive {
            println!("📂 Opening folder dialog for the next destination...");
            if let Some(new_dst) = rfd::FileDialog::new()
                .set_title("Select Destination Folder for Next Transfer")
                .pick_folder()
            {
                println!("Selected Next Destination: {:?}", new_dst);
                dst_dir = new_dst;
            } else {
                println!("❌ Folder selection cancelled. Exiting loop mode.");
                break;
            }
        }
    }

    stop_broadcaster.store(true, Ordering::Relaxed);
    Ok(())
}
