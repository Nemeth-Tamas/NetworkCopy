use crate::protocol;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// Runs the server-side speedtest loop, listening and measuring client data bursts.
pub fn run_speedtest_server(control_stream: &mut TcpStream, listener: &TcpListener) -> std::io::Result<()> {
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
        let mut sockets = Vec::with_capacity(k);
        for _ in 0..k {
            let (mut socket, _) = listener.accept()?;

            // Verify data stream magic bytes
            let mut magic = [0u8; 4];
            socket.read_exact(&mut magic)?;
            if &magic != b"FSTD" {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid speedtest stream magic",
                ));
            }

            // Verify stream index (has speedtest bit set)
            let mut idx_bytes = [0u8; 4];
            socket.read_exact(&mut idx_bytes)?;
            let idx = u32::from_be_bytes(idx_bytes);
            if idx & 0x8000_0000 == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected speedtest stream bit",
                ));
            }
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

/// Spawns a background thread broadcasting FSTP-RECEIVER presence over UDP.
pub fn start_udp_broadcaster(listen_port: u16, stop_flag: Arc<AtomicBool>) -> std::io::Result<()> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.set_broadcast(true)?;

    thread::spawn(move || {
        let message = format!("FSTP-RECEIVER:{}", listen_port);
        let broadcast_addr = "255.255.255.255:7879";
        while !stop_flag.load(Ordering::Relaxed) {
            let _ = socket.send_to(message.as_bytes(), broadcast_addr);
            thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    Ok(())
}

pub fn run_receiver(dst_dir: PathBuf, listen_addr: &str) -> std::io::Result<()> {
    // Start UDP broadcaster in background
    let stop_broadcaster = Arc::new(AtomicBool::new(false));
    let stop_broadcaster_clone = Arc::clone(&stop_broadcaster);
    // Parse port from listen_addr (default 7878 if parsing fails)
    let port = listen_addr.split(':').last().and_then(|p| p.parse::<u16>().ok()).unwrap_or(7878);
    start_udp_broadcaster(port, stop_broadcaster_clone)?;

    println!("👂 Listening for connection on {}...", listen_addr);
    let listener = TcpListener::bind(listen_addr)?;

    // Accept control connection
    let (mut control_stream, client_addr) = listener.accept()?;
    println!("🔌 Connection established with client: {}", client_addr);

    // Read control magic bytes
    let mut magic = [0u8; 4];
    control_stream.read_exact(&mut magic)?;
    if &magic != b"FSTP" {
        stop_broadcaster.store(true, Ordering::Relaxed);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid control stream magic bytes",
        ));
    }

    // Read mode
    let mut mode = [0u8; 1];
    control_stream.read_exact(&mut mode)?;
    if mode[0] != 1 {
        stop_broadcaster.store(true, Ordering::Relaxed);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid transfer mode",
        ));
    }

    // Run speedtest server logic (it will loop until command 0 is received)
    if let Err(e) = run_speedtest_server(&mut control_stream, &listener) {
        stop_broadcaster.store(true, Ordering::Relaxed);
        return Err(e);
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

    println!("📥 Receiving file index...");
    let files = protocol::read_index(&mut control_stream)?;

    // Smart Resume: Check existing files
    println!("🔍 Checking destination for files to skip (smart resume)...");
    let mut to_transfer_indices = Vec::new();
    for (idx, file) in files.iter().enumerate() {
        let full_path = dst_dir.join(&file.rel_path);
        let mut skip = false;
        if full_path.exists() {
            if let Ok(meta) = std::fs::metadata(&full_path) {
                if meta.is_file() && meta.len() == file.size {
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
        if !skip {
            to_transfer_indices.push(idx as u32);
        }
    }

    // Send transfer list back to Sender
    protocol::write_transfer_list(&mut control_stream, &to_transfer_indices)?;

    // Filter index list
    let mut files_to_transfer = Vec::with_capacity(to_transfer_indices.len());
    for &idx in &to_transfer_indices {
        files_to_transfer.push(files[idx as usize].clone());
    }

    let total_bytes_to_transfer: u64 = files_to_transfer.iter().map(|f| f.size).sum();
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
    let dst_dir_arc = Arc::new(dst_dir);
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

        let handle = thread::spawn(move || -> std::io::Result<()> {
            let mut buffer = [0u8; 64 * 1024]; // 64KB buffer

            // Wrap in decompression decoder if enabled
            let mut reader = if use_compression {
                protocol::StreamReader::Compressed(lz4_flex::frame::FrameDecoder::new(socket))
            } else {
                protocol::StreamReader::Raw(socket)
            };

            for file_entry in bucket {
                if file_entry.size == 0 {
                    continue;
                }

                let full_path = dst_dir.join(&file_entry.rel_path);
                let tmp_path = full_path.with_extension("networkcopy-tmp");
                
                let file = File::create(&tmp_path)?;
                let mut writer = BufWriter::new(file);
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
                writer.flush()?;
                drop(writer); // Close file handle

                // Read and verify 4-byte CRC32 checksum
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
    stop_broadcaster.store(true, Ordering::Relaxed);

    if !thread_errors.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Transfer failed with errors: {:?}", thread_errors),
        ));
    }

    println!("🎉 All streams received and files written successfully!");
    Ok(())
}
