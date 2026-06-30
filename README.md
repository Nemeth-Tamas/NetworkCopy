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
./networkcopy.exe receive "C:\DestFolder"

# On the source machine (Sender):
./networkcopy.exe send "C:\SourceFolder" --ip 192.168.1.100 --streams 8 --compress
```

*Arguments:*
- `send <path>` / `receive <path>`: Source or destination paths.
- `--ip <ip>`: Target Receiver IP address (defaults to `127.0.0.1` if not found via UDP).
- `--streams <n>`: Override stream count (defaults to `0` for auto-tuning).
- `--compress`: Enable LZ4 compression.

### 🖱️ Interactive Mode

Run the executable without arguments to enter the interactive menu:

1. Select option `1` (Sender) or `2` (Receiver).
2. A native folder picker dialog will prompt you to select the directory.
3. If running as Sender, it will scan for UDP broadcasts on port `7879`. If it finds a receiver, it asks: `✨ Auto-discovered Receiver at 192.168.1.105:7878! Connect? (Y/n)`.
4. Choose whether to enable LZ4 compression and enter stream counts.

---

## 🧪 Testing

We include a comprehensive multi-threaded integration test suite testing:
1. Directory walking and skipping exclusively locked files.
2. Full raw loopback transfer of a **5.00 GB** dataset (including 2,000 small files, 5 large 1GB files, and an empty file).
3. Smart Resume (skipping all files on a secondary run).
4. Partial Smart Resume (transferring only 1 modified and 1 new file using LZ4 compression).
5. CRC32 byte-for-byte integrity.

To run the test and output real-time progress:
```powershell
cargo test -- --nocapture
```

---

## 🗺️ Roadmap & Future TODOs

### 🎯 v1.1 (Polish & Usability)
- [ ] **Path Sanitization**: Reject unsafe relative path tricks (like `..`, absolute paths, Windows reserved names).
- [ ] **Improved Temp Files**: Append `.networkcopy-tmp` to avoid extension stripping.
- [ ] **Clap CLI**: Full argument parser with configurable ports, binds, streams, and compress options.
- [ ] **Interactive Receiver Loop Mode**: Loop option that prompts receiver for destination folder per transfer.
- [ ] **Robust Smart Resume**: `--verify-existing` to checksum existing destination files via CRC32 before skipping.
- [ ] **Dry Run Mode**: `--dry-run` to dry-run scanning, skip detection, and bucket splitting.
- [ ] **Include/Exclude Filters**: Wildcard filters (e.g. `--exclude node_modules`, `--include "*.jpg"`).
- [ ] **Pairing Code Verification**: 4-digit numeric code pairing confirmation.
- [ ] **HMAC Session Authentication**: Opt-in `--auth <key>` mode signing control packets & discovery via HMAC-SHA256.

### 🚀 v2.0 (Serious Capability)
- [ ] **Linux Cross-Platform**: Win-to-Win, Linux-to-Linux, Win-to-Linux, and Linux-to-Win path and permission handling.
- [ ] **Partial File Resume**: Continue copying interrupted transfers from `.tmp` byte offset.
- [ ] **Protocol Versioning**: Handshake checks for protocol versions and supported features.
- [ ] **Transfer Presets & Manifests**: Save jobs (excludes, directories, targets) as reusable configurations.
- [ ] **TUI (Terminal User Interface)**: Rich live terminal display of streams, speeds, and file progress.
- [ ] **LAN Benchmark Mode**: Network-only speed testing without disk writes.

### 🔒 v2.1 (Optional Hardening)
- [ ] **Opt-in Transfer Encryption**: `--encrypt` mode using ChaCha20-Poly1305 or AES-GCM (implies `--auth` and requires pairing code).

