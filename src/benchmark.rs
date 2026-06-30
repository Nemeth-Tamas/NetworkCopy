use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::encrypted_stream::{MaybeEncryptedStream, EncryptedStream};

/// Runs the client-side benchmark by flooding the receiver with dummy data.
pub fn run_benchmark_client(
    receiver_addr: &str,
    num_streams: usize,
    duration_secs: u32,
    mut control_stream: MaybeEncryptedStream<TcpStream>,
    use_encryption: bool,
    session_key: [u8; 32],
) -> std::io::Result<()> {
    // Send selected stream count (4 bytes) and duration (4 bytes)
    control_stream.write_all(&(num_streams as u32).to_be_bytes())?;
    control_stream.write_all(&(duration_secs as u32).to_be_bytes())?;
    control_stream.flush()?;

    // Read ready confirmation from server
    let mut ready = [0u8; 1];
    control_stream.read_exact(&mut ready)?;
    if ready[0] != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "Receiver refused benchmark session",
        ));
    }

    println!("⚡ Spawning {} parallel streams for benchmarking...", num_streams);
    let stop_signal = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(num_streams);
    let mut sockets = Vec::with_capacity(num_streams);

    // Pre-connect all streams
    let mut raw_sockets = Vec::with_capacity(num_streams);
    for stream_idx in 0..num_streams {
        let mut raw_socket = TcpStream::connect(receiver_addr)?;
        raw_sockets.push(raw_socket.try_clone()?);
        raw_socket.write_all(b"FSTB")?;
        raw_socket.write_all(&(stream_idx as u32).to_be_bytes())?;
        raw_socket.flush()?;

        let socket = if use_encryption {
            MaybeEncryptedStream::Encrypted(EncryptedStream::new(
                raw_socket,
                session_key,
                (stream_idx as u32) + 1,
                ((stream_idx as u32) + 1) | 0x8000_0000,
            ))
        } else {
            MaybeEncryptedStream::Raw(raw_socket)
        };
        sockets.push(socket);
    }

    let bytes_sent = Arc::new(AtomicU64::new(0));

    // Spawn threads to flood the streams with dummy data
    for mut socket in sockets {
        let stop = Arc::clone(&stop_signal);
        let sent_counter = Arc::clone(&bytes_sent);
        let handle = thread::spawn(move || -> std::io::Result<()> {
            let buffer = [0u8; 64 * 1024]; // 64KB dummy buffer
            while !stop.load(Ordering::Relaxed) {
                socket.write_all(&buffer)?;
                sent_counter.fetch_add(buffer.len() as u64, Ordering::Relaxed);
            }
            Ok(())
        });
        handles.push(handle);
    }

    let start_time = Instant::now();
    let mut last_bytes = 0u64;

    for sec in 1..=duration_secs {
        thread::sleep(Duration::from_secs(1));
        let total = bytes_sent.load(Ordering::Relaxed);
        let diff = total - last_bytes;
        last_bytes = total;
        let speed = diff as f64 / 1_048_576.0;
        println!("🚀 [Client] Sec {:2}: {:.2} MB/s (Sent: {:.2} MB)", sec, speed, total as f64 / 1_048_576.0);
    }

    stop_signal.store(true, Ordering::Relaxed);

    // Explicitly shut down all raw sockets to unblock the flood threads instantly
    for raw_socket in raw_sockets {
        let _ = raw_socket.shutdown(std::net::Shutdown::Both);
    }

    for handle in handles {
        let _ = handle.join();
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    let total = bytes_sent.load(Ordering::Relaxed);
    let avg_speed = (total as f64 / elapsed) / 1_048_576.0;
    println!("\n🏆 [Benchmark Summary]");
    println!("⏱️ Duration: {:.2} seconds", elapsed);
    println!("📊 Total Data Sent: {:.2} MB", total as f64 / 1_048_576.0);
    println!("⚡ Average Speed: {:.2} MB/s\n", avg_speed);

    Ok(())
}

/// Runs the server-side benchmark by reading and discarding incoming data.
pub fn run_speedtest_benchmark_server(
    control_stream: &mut MaybeEncryptedStream<TcpStream>,
    listener: &TcpListener,
    use_encryption: bool,
    session_key: [u8; 32],
) -> std::io::Result<()> {
    // Read stream count (4 bytes) and duration (4 bytes)
    let mut stream_count_bytes = [0u8; 4];
    control_stream.read_exact(&mut stream_count_bytes)?;
    let k = u32::from_be_bytes(stream_count_bytes) as usize;

    let mut duration_bytes = [0u8; 4];
    control_stream.read_exact(&mut duration_bytes)?;
    let duration_secs = u32::from_be_bytes(duration_bytes);

    // Send confirmation
    control_stream.write_all(&[1u8])?;
    control_stream.flush()?;

    println!("⚡ Preparing benchmark receiver for {} streams...", k);
    let mut raw_sockets = Vec::with_capacity(k);
    let mut sockets = Vec::with_capacity(k);
    for _ in 0..k {
        let (mut raw_socket, _) = listener.accept()?;
        raw_sockets.push(raw_socket.try_clone()?);
        let mut magic = [0u8; 4];
        raw_socket.read_exact(&mut magic)?;
        if &magic != b"FSTB" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid benchmark stream magic",
            ));
        }
        let mut idx_bytes = [0u8; 4];
        raw_socket.read_exact(&mut idx_bytes)?;
        let stream_idx = u32::from_be_bytes(idx_bytes) as usize;

        let socket = if use_encryption {
            MaybeEncryptedStream::Encrypted(EncryptedStream::new(
                raw_socket,
                session_key,
                ((stream_idx as u32) + 1) | 0x8000_0000,
                (stream_idx as u32) + 1,
            ))
        } else {
            MaybeEncryptedStream::Raw(raw_socket)
        };
        sockets.push(socket);
    }

    println!("🚀 Starting benchmark measurement for {} seconds...", duration_secs);
    let bytes_received = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(k);
    let start_time = Instant::now();

    for mut socket in sockets {
        let bytes_counter = Arc::clone(&bytes_received);
        let handle = thread::spawn(move || -> std::io::Result<()> {
            let mut buffer = [0u8; 64 * 1024];
            loop {
                match socket.read(&mut buffer) {
                    Ok(0) => break,
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

    let mut last_bytes = 0u64;
    for sec in 1..=duration_secs {
        thread::sleep(Duration::from_secs(1));
        let total = bytes_received.load(Ordering::Relaxed);
        let diff = total - last_bytes;
        last_bytes = total;
        let speed = diff as f64 / 1_048_576.0;
        println!("📥 [Server] Sec {:2}: {:.2} MB/s (Received: {:.2} MB)", sec, speed, total as f64 / 1_048_576.0);
    }

    // Explicitly shut down all accepted sockets to unblock the discard threads
    for raw_socket in raw_sockets {
        let _ = raw_socket.shutdown(std::net::Shutdown::Both);
    }

    // Join threads
    for handle in handles {
        let _ = handle.join();
    }

    let elapsed = start_time.elapsed().as_secs_f64();
    let total = bytes_received.load(Ordering::Relaxed);
    let avg_speed = (total as f64 / elapsed) / 1_048_576.0;
    println!("\n🏆 [Benchmark Summary]");
    println!("⏱️ Duration: {:.2} seconds", elapsed);
    println!("📊 Total Data Received: {:.2} MB", total as f64 / 1_048_576.0);
    println!("⚡ Average Speed: {:.2} MB/s\n", avg_speed);

    Ok(())
}
