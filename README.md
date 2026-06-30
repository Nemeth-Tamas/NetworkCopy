# 🚀 NetworkCopy

A high-performance, memory-efficient, multi-stream P2P file sender written in Rust, designed to saturate local network connections. It is optimized to run efficiently even on older hardware (e.g., low-RAM Windows 11 dual-core/quad-core machines).

> [!NOTE]
> **Co-Authorship**: This project was co-authored pair-programming style by a developer and **Antigravity**, an advanced agentic AI coding assistant designed by Google DeepMind.

---

## 🛡️ Trust & Security Model
NetworkCopy is designed primarily as a **trusted-LAN high-speed file copy utility**. 
- **Default Mode**: Speed-focused, unencrypted, and unauthenticated to maximize network saturation and minimize CPU/memory overhead on older hardware.
- **Opt-In Security**: Secure configurations (such as HMAC metadata authentication `--auth` and pairing codes) are fully opt-in via CLI flags and interactive prompts, allowing you to trade minimal speed for trust validation when copying over untrusted or shared local networks.

---

## 🛠️ How It Works (Design & Architecture)

To bypass the typical slowdowns associated with standard file-sharing protocols (which suffer from start-stop overhead, disk-seeking latency on tiny files, and single-stream network bottlenecks), NetworkCopy uses a custom execution flow:

1. **Parallel Directory Indexing**: Uses `rayon` to scan the source directory recursively in parallel, generating an in-memory index of all relative paths, file sizes, and modification timestamps.
2. **Robust Scan Filter**: During directory walking, any locked files or permission-restricted files are gracefully skipped with a console warning instead of aborting the transfer.
3. **UDP Auto-Discovery**: The Receiver broadcasts its presence on the network, allowing the Sender to auto-discover and connect with a single click.
4. **Auto-Tuning Speedtest**: Prior to transfer, the client runs a 500ms sequential speedtest over 1, 2, 4, and 8 parallel TCP streams. It determines the highest throughput concurrency configuration and configures the transfer dynamically.
5. **Smart Resume**: The Receiver compares file sizes and modification times with the Sender's index. Existing matching files are skipped entirely, and original modification timestamps are preserved on the destination.
6. **Deterministic Partitioning**: Both the Sender and Receiver run the exact same greedy partitioning algorithm (Longest Processing Time first) to balance the remaining file list into $N$ active buckets.
7. **Pipelined LZ4 Compression & CRC32 Checks**: Data is streamed in 64KB chunks through a compile-time statically dispatched pipeline (avoiding heap allocations). Data can be optionally compressed with LZ4 and is verified on-the-fly using CRC32 checksums appended to each stream.
8. **Atomic Renaming**: Received data is written to `.networkcopy-tmp` files and atomically renamed to their final names only *after* the CRC32 check passes, preventing partial file corruption.

---

## 📋 Features

- **UDP Auto-Discovery**: Connect to your receiver automatically without typing local IP addresses (includes manual entry backup).
- **Auto-Tuning Concurrency**: Speedtest auto-tunes the stream count (1, 2, 4, or 8) to fully saturate your network adapter.
- **Smart Resume / Metadata Preservation**: Skips files that already exist in the destination with the exact same path, size, and modified time, while preserving original timestamps.
- **LZ4 Stream Compression**: Optional on-the-fly compression, statically dispatched to minimize CPU/memory overhead on old dual-core machines.
- **CRC32 Integrity Validation**: Verify file integrity on-the-fly with zero extra disk reads.
- **Atomic Temp File Renaming**: Stream into `.networkcopy-tmp` and rename on CRC32 success.
- **Robustness on Windows**: Skips locked/system files with a warning instead of aborting the index.
- **Scriptable CLI / Headless Mode**: Bypass GUI dialogs for ssh/headless environments, or fall back to interactive GUI native file pickers if no args are passed.

---

## 🚀 Usage
 
 ### 💻 Command Line Interface (CLI)
 
 Run headlessly or script with parameters:
 
 ```powershell
 # On the destination machine (Receiver):
 ./networkcopy.exe receive <dst_dir> [options]
 
 # On the source machine (Sender):
 ./networkcopy.exe send <src_dir> [options]
 ```
 
 #### CLI Options Summary
 
 **`send` command options:**
 - `src_dir`: Directory containing source files.
 - `--ip <ip>`: Target Receiver IP address. If omitted, uses UDP auto-discovery.
 - `--port <port>`: Control connection TCP port (default: `7878`).
 - `--streams <n>`: Parallel streams count (default: `0` for auto-tune speedtest).
 - `--compress`: Enable LZ4 compression on data streams.
 - `--no-discovery`: Skip UDP auto-discovery search.
 - `--yes`: Bypass all interactive confirmation prompts.
 - `--verify-existing`: Verify existing files using CRC32 checksums instead of modification times.
 - `--auth <key>`: HMAC-SHA256 pre-shared secret key for secure transmission.
 - `--dry-run`: Performs scan and reports skipped/transferred files and stream partition buckets without transferring data.
 - `--include <pattern>`: Glob patterns to include in scanning (e.g. `*.rs`).
 - `--exclude <pattern>`: Glob patterns to exclude from scanning (e.g. `target/*`).
 - `--discovery-port <port>`: UDP discovery port (default: `7879`).
 
 **`receive` command options:**
 - `dst_dir`: Destination directory.
 - `--port <port>`: Port to bind TCP listener (default: `7878`).
 - `--bind <ip>`: IP address to bind listener (default: `0.0.0.0`).
 - `--verify-existing`: Verify existing files using CRC32 checksums instead of modification times.
 - `--loop-mode`: Persistent mode (loops to wait for next transfer).
 - `--auth <key>`: HMAC-SHA256 pre-shared secret key for secure transmission.
 - `--discovery-port <port>`: UDP discovery port (default: `7879`).
 - `--yes`: Bypass interactive prompts (e.g. folder picker in loop mode).

 **`preset` command options:**
 - `path`: Path to JSON preset configuration file (defines a send or receive job).

 **`benchmark` command options:**
 - `--ip <ip>`: Target Receiver IP. If specified, runs as benchmark client. If omitted, runs as benchmark server.
 - `--port <port>`: Port to connect or bind to (default: `7878`).
 - `--streams <n>`: Number of parallel streams to flood (default: `8`).
 - `--duration <secs>`: Duration of the benchmark test in seconds (default: `5`).
 - `--yes`: Bypass interactive pairing/auth prompts.
 - `--auth <key>`: HMAC authentication key.
 
 #### Examples
 
 1. **Secure Copy (HMAC Authentication)**:
    ```powershell
    # Receiver:
    ./networkcopy.exe receive "C:\Dest" --auth "MySecretKey"
    
    # Sender:
    ./networkcopy.exe send "C:\Source" --auth "MySecretKey"
    ```
 
 2. **Filtered & Verified Copy (Exclude target folder and verify using CRC32)**:
    ```powershell
    # Receiver:
    ./networkcopy.exe receive "C:\Dest" --verify-existing
    
    # Sender:
    ./networkcopy.exe send "C:\Source" --exclude "target/*" --exclude "node_modules/*" --verify-existing
    ```
 
 3. **Dry-Run Analysis**:
    ```powershell
    ./networkcopy.exe send "C:\Source" --dry-run
    ```
 
 4. **Persistent Loop Mode (IT shop automation)**:
    ```powershell
    ./networkcopy.exe receive "C:\Incoming" --loop-mode
    ```

 5. **JSON Job Presets**:
    Create a preset configuration file `transfer_job.json`:
    ```json
    {
      "role": "send",
      "path": "C:\\Source",
      "ip": "192.168.1.50",
      "streams": 8,
      "compress": true,
      "yes": true
    }
    ```
    Execute the preset job:
    ```powershell
    ./networkcopy.exe preset transfer_job.json
    ```

 6. **LAN Benchmark Mode (Network Speed Testing without Disk IO)**:
    ```powershell
    # Receiver (Server):
    ./networkcopy.exe benchmark
    
    # Sender (Client):
    ./networkcopy.exe benchmark --ip 192.168.1.50 --duration 10
    ```
 
 ### 🖱️ Interactive Mode
 
 Run the executable without arguments to enter the interactive menu:
 
 1. Select option `1` (Sender) or `2` (Receiver).
 2. A native folder picker dialog will prompt you to select the directory.
 3. If running as Sender, it will scan for UDP broadcasts on port `7879`. If it finds a receiver, it asks: `✨ Auto-discovered Receiver at 192.168.1.105:7878! Connect? (Y/n)`.
 4. Enter pairing code generated on the receiver if interactive.
 5. Choose whether to enable LZ4 compression and enter stream counts.
 
 ---
 
 ## 🧪 Testing
 
 We include a comprehensive multi-threaded integration test suite testing:
 1. Path safety rules validation.
 2. Include/exclude glob filters.
 3. HMAC-SHA256 authentication (matching and mismatching keys).
 4. Dry-run scanning and skipped analysis.
 5. Interactive pairing code verification.
 6. Directory walking and skipping exclusively locked files.
 7. Full raw loopback transfer of a **5.00 GB** dataset (including 2,000 small files, 5 large 1GB files, and an empty file).
 8. Smart Resume (skipping all files on a secondary run).
 9. Partial Smart Resume (transferring only 1 modified and 1 new file using LZ4 compression).
 10. CRC32 byte-for-byte integrity.
 
 To run the test and output real-time progress:
 ```powershell
 cargo test -- --nocapture
 ```
 
 ---
 
 ## 🗺️ Roadmap & Future TODOs
 
 ### 🎯 v1.1 (Polish & Usability)
 - [x] **Path Sanitization**: Reject unsafe relative path tricks (like `..`, absolute paths, Windows reserved names).
 - [x] **Improved Temp Files**: Append `.networkcopy-tmp` to avoid extension stripping.
 - [x] **Clap CLI**: Full argument parser with configurable ports, binds, streams, and compress options.
 - [x] **Interactive Receiver Loop Mode**: Loop option that prompts receiver for destination folder per transfer.
 - [x] **Robust Smart Resume**: `--verify-existing` to checksum existing destination files via CRC32 before skipping.
 - [x] **Dry Run Mode**: `--dry-run` to dry-run scanning, skip detection, and bucket splitting.
 - [x] **Include/Exclude Filters**: Wildcard filters (e.g. `--exclude node_modules`, `--include "*.jpg"`).
 - [x] **Pairing Code Verification**: 4-digit numeric code pairing confirmation.
 - [x] **HMAC Session Authentication**: Opt-in `--auth <key>` mode signing control packets & discovery via HMAC-SHA256.

### 🚀 v2.0 (Serious Capability)
 - [x] **Linux Cross-Platform**: Win-to-Win, Linux-to-Linux, Win-to-Linux, and Linux-to-Win path and permission handling.
 - [x] **Partial File Resume**: Continue copying interrupted transfers from `.tmp` byte offset.
 - [x] **Protocol Versioning**: Handshake checks for protocol versions and supported features.
 - [x] **Transfer Presets & Manifests**: Save jobs (excludes, directories, targets) as reusable configurations.
 - [x] **TUI (Terminal User Interface)**: Rich live terminal display of streams, speeds, and file progress (indicatif integration).
 - [x] **LAN Benchmark Mode**: Network-only speed testing without disk writes.
 
 ### 🔒 v2.1 (Optional Hardening)
 - [ ] **Opt-in Transfer Encryption**: `--encrypt` mode using ChaCha20-Poly1305 or AES-GCM (implies `--auth` and requires pairing code).
