use clap::{Parser, Subcommand};
use networkcopy::{receiver::{self, ReceiverOptions}, sender::{self, SenderOptions}, benchmark, preset::run_preset};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "networkcopy", version = "2.2.0", about = "Fast P2P Parallel File Copy Tool")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Mode choice: 1 for Sender, 2 for Receiver (interactive fallback)
    #[arg(short, long)]
    mode: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Send files to a receiver
    Send {
        /// Source directory to copy files from
        src_dir: PathBuf,

        /// Receiver IP or hostname (e.g. 192.168.1.50)
        #[arg(long)]
        ip: Option<String>,

        /// TCP Port for the control connection (default: 7878)
        #[arg(long, default_value_t = 7878)]
        port: u16,

        /// Number of parallel streams (0 = auto-tune with speedtest)
        #[arg(short, long, default_value_t = 0)]
        streams: usize,

        /// Enable LZ4 compression on transfer streams
        #[arg(short, long)]
        compress: bool,

        /// Skip UDP auto-discovery
        #[arg(long)]
        no_discovery: bool,

        /// Bypass all interactive prompts (auto-confirm transfers/prompts)
        #[arg(short, long)]
        yes: bool,

        /// Verify files using CRC32 checksums instead of modification times
        #[arg(long)]
        verify_existing: bool,

        /// HMAC SHA256 pre-shared secret key for secure transmission
        #[arg(long)]
        auth: Option<String>,

        /// Perform a dry-run (scans and shows stats without copying files)
        #[arg(long)]
        dry_run: bool,

        /// Glob patterns to include in transfer (e.g. '*.rs')
        #[arg(long)]
        include: Vec<String>,

        /// Glob patterns to exclude from transfer (e.g. 'target/*')
        #[arg(long)]
        exclude: Vec<String>,

        /// UDP discovery port (default: 7879)
        #[arg(long, default_value_t = 7879)]
        discovery_port: u16,

        /// Enable ChaCha20-Poly1305 stream encryption
        #[arg(long)]
        encrypt: bool,
    },
    /// Receive files from a sender
    Receive {
        /// Destination directory to copy files to
        dst_dir: PathBuf,

        /// Port to bind the control TCP listener (default: 7878)
        #[arg(long, default_value_t = 7878)]
        port: u16,

        /// IP address to bind (default: 0.0.0.0)
        #[arg(long, default_value = "0.0.0.0")]
        bind: String,

        /// Verify files using CRC32 checksums instead of modification times
        #[arg(long)]
        verify_existing: bool,

        /// Run in loop mode (accepts multiple consecutive transfer sessions)
        #[arg(long)]
        loop_mode: bool,

        /// HMAC SHA256 pre-shared secret key for secure transmission
        #[arg(long)]
        auth: Option<String>,

        /// UDP discovery port (default: 7879)
        #[arg(long, default_value_t = 7879)]
        discovery_port: u16,
        
        /// Bypass interactive prompts (e.g. bypass folder picker in loop mode)
        #[arg(short, long)]
        yes: bool,

        /// Enable ChaCha20-Poly1305 stream encryption
        #[arg(long)]
        encrypt: bool,
    },
    /// Run a transfer job defined in a JSON preset file
    Preset {
        /// Path to the JSON preset file
        path: PathBuf,
    },
    /// Run network benchmark to test network bandwidth without writing to disk
    Benchmark {
        /// Target IP or hostname (runs as client if specified, server if omitted)
        #[arg(long)]
        ip: Option<String>,

        /// TCP Port to connect or bind to (default: 7878)
        #[arg(long, default_value_t = 7878)]
        port: u16,

        /// Number of parallel streams (default: 8)
        #[arg(short, long, default_value_t = 8)]
        streams: usize,

        /// Duration of the benchmark in seconds (default: 5)
        #[arg(short, long, default_value_t = 5)]
        duration: u32,

        /// Bypass interactive pairing/auth prompts (runs automatically)
        #[arg(short, long)]
        yes: bool,

        /// HMAC SHA256 pre-shared secret key for secure benchmarking
        #[arg(long)]
        auth: Option<String>,

        /// Enable ChaCha20-Poly1305 stream encryption
        #[arg(long)]
        encrypt: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Send {
            src_dir,
            ip,
            port,
            streams,
            compress,
            no_discovery,
            yes,
            verify_existing,
            auth,
            dry_run,
            include,
            exclude,
            discovery_port,
            encrypt,
        }) => {
            if encrypt && auth.is_none() {
                eprintln!("❌ Error: Encryption requires authentication. Please provide an authentication key via `--auth <key>`.");
                std::process::exit(1);
            }
            let mut target_addr = None;

            if !no_discovery && ip.is_none() {
                if let Ok(Some(discovered)) = sender::discover_receiver(discovery_port, auth.clone()) {
                    println!("✨ Auto-discovered Receiver at {}!", discovered);
                    let mut proceed = true;
                    if !yes {
                        print!("Connect to this Receiver? (Y/n): ");
                        io::stdout().flush().unwrap();
                        let mut ans = String::new();
                        io::stdin().read_line(&mut ans).unwrap();
                        let ans = ans.trim().to_lowercase();
                        if !ans.is_empty() && !ans.starts_with('y') {
                            proceed = false;
                        }
                    }
                    if proceed {
                        target_addr = Some(discovered);
                    }
                }
            }

            let receiver_addr = if let Some(addr) = target_addr {
                addr
            } else {
                let ip_str = ip.unwrap_or_else(|| {
                    if yes {
                        "127.0.0.1".to_string()
                    } else {
                        print!("Enter Receiver IP address (default: 127.0.0.1): ");
                        io::stdout().flush().unwrap();
                        let mut ip_input = String::new();
                        io::stdin().read_line(&mut ip_input).unwrap();
                        let ip_trimmed = ip_input.trim();
                        if ip_trimmed.is_empty() {
                            "127.0.0.1".to_string()
                        } else {
                            ip_trimmed.to_string()
                        }
                    }
                });
                format!("{}:{}", ip_str, port)
            };

            let options = SenderOptions {
                includes: include,
                excludes: exclude,
                verify_existing,
                dry_run,
                no_discovery,
                auth_key: auth,
                control_port: port,
                discovery_port,
                auto_accept: yes,
                pairing_code: None,
                encrypt,
            };

            println!("🚀 Running in CLI Sender mode...");
            println!("📂 Source: {:?}", src_dir);
            println!("🔌 Target: {}", receiver_addr);
            println!("🗜️ LZ4 Compression: {}", compress);
            if let Err(e) = sender::run_sender(src_dir, &receiver_addr, streams, compress, options) {
                eprintln!("❌ Sender Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::Receive {
            dst_dir,
            port,
            bind,
            verify_existing,
            loop_mode,
            auth,
            discovery_port,
            yes,
            encrypt,
        }) => {
            if encrypt && auth.is_none() {
                eprintln!("❌ Error: Encryption requires authentication. Please provide an authentication key via `--auth <key>`.");
                std::process::exit(1);
            }
            let listen_addr = format!("{}:{}", bind, port);
            let options = ReceiverOptions {
                verify_existing,
                loop_mode,
                auth_key: auth,
                control_port: port,
                discovery_port,
                pairing_code: None,
                encrypt,
            };

            println!("🚀 Running in CLI Receiver mode...");
            println!("📂 Destination: {:?}", dst_dir);
            println!("👂 Listening on {}...", listen_addr);
            if let Err(e) = receiver::run_receiver(dst_dir, &listen_addr, !yes, options) {
                eprintln!("❌ Receiver Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::Preset { path }) => {
            if let Err(e) = run_preset(path) {
                eprintln!("❌ Preset Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::Benchmark {
            ip,
            port,
            streams,
            duration,
            yes,
            auth,
            encrypt,
        }) => {
            use std::io::Read;
            if encrypt && auth.is_none() {
                eprintln!("❌ Error: Encryption requires authentication. Please provide an authentication key via `--auth <key>`.");
                std::process::exit(1);
            }
            if let Some(target_ip) = ip {
                let receiver_addr = format!("{}:{}", target_ip, port);
                let options = SenderOptions {
                    auth_key: auth,
                    control_port: port,
                    auto_accept: yes,
                    encrypt,
                    ..Default::default()
                };
                println!("🚀 Launching LAN benchmark client against {}...", receiver_addr);
                if let Err(e) = sender::run_benchmark_sender(&receiver_addr, streams, duration, options) {
                    eprintln!("❌ Benchmark Client Error: {}", e);
                    std::process::exit(1);
                }
            } else {
                let bind_addr = format!("0.0.0.0:{}", port);
                let listener = match std::net::TcpListener::bind(&bind_addr) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("❌ Failed to bind TCP listener on {}: {}", bind_addr, e);
                        std::process::exit(1);
                    }
                };
                let options = ReceiverOptions {
                    auth_key: auth,
                    control_port: port,
                    encrypt,
                    ..Default::default()
                };

                println!("🚀 Launching LAN benchmark receiver. Listening on {}...", bind_addr);
                loop {
                    let (mut control_stream, client_addr) = match listener.accept() {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("⚠️ Failed to accept connection: {}", e);
                            continue;
                        }
                    };
                    println!("🔌 Control stream connected from client: {}", client_addr);

                    // Read magic bytes
                    let mut magic = [0u8; 4];
                    if control_stream.read_exact(&mut magic).is_err() { continue; }
                    if &magic != b"FSTP" {
                        eprintln!("⚠️ Invalid control stream magic bytes");
                        continue;
                    }

                    // Read mode (expects 3 for benchmark)
                    let mut mode = [0u8; 1];
                    if control_stream.read_exact(&mut mode).is_err() { continue; }
                    if mode[0] != 3 {
                        eprintln!("⚠️ Invalid mode for benchmark: expected 3, got {}", mode[0]);
                        continue;
                    }

                    // Read protocol version (expects 2)
                    let mut version = [0u8; 1];
                    if control_stream.read_exact(&mut version).is_err() { continue; }
                    if version[0] != 2 {
                        eprintln!("⚠️ Protocol version mismatch: expected 2, got {}", version[0]);
                        continue;
                    }

                    // Read encrypt flag
                    let mut encrypt_flag = [0u8; 1];
                    if control_stream.read_exact(&mut encrypt_flag).is_err() { continue; }
                    let client_wants_encrypt = encrypt_flag[0] == 1;

                    if client_wants_encrypt != options.encrypt {
                        eprintln!("⚠️ Encryption settings mismatch: client wants encrypt = {}, server config = {}", client_wants_encrypt, options.encrypt);
                        continue;
                    }

                    let mut session_key = [0u8; 32];
                    if options.encrypt {
                        let mut sender_nonce = [0u8; 32];
                        if control_stream.read_exact(&mut sender_nonce).is_err() { continue; }

                        use rand::RngCore;
                        let mut receiver_nonce = [0u8; 32];
                        rand::thread_rng().fill_bytes(&mut receiver_nonce);
                        if control_stream.write_all(&receiver_nonce).is_err() { continue; }
                        let _ = control_stream.flush();

                        use hmac::Mac;
                        let mut mac = hmac::SimpleHmac::<sha2::Sha256>::new_from_slice(options.auth_key.as_ref().unwrap().as_bytes()).unwrap();
                        mac.update(&sender_nonce);
                        mac.update(&receiver_nonce);
                        session_key = mac.finalize().into_bytes().into();
                    }

                    use networkcopy::encrypted_stream::{MaybeEncryptedStream, EncryptedStream};
                    let mut control_stream = if options.encrypt {
                        MaybeEncryptedStream::Encrypted(EncryptedStream::new(control_stream, session_key, 0x8000_0000, 0))
                    } else {
                        MaybeEncryptedStream::Raw(control_stream)
                    };

                    // HMAC Auth Handshake
                    let auth_required = options.auth_key.is_some();
                    if control_stream.write_all(&[if auth_required { 1 } else { 0 }]).is_err() { continue; }
                    let _ = control_stream.flush();

                    if auth_required {
                        let key = options.auth_key.as_ref().unwrap();
                        let challenge = receiver::generate_challenge();
                        if control_stream.write_all(&challenge).is_err() { continue; }
                        let mut response = [0u8; 32];
                        if control_stream.read_exact(&mut response).is_err() { continue; }
                        let expected = networkcopy::protocol::compute_hmac(key, &challenge);
                        if expected == response {
                            if control_stream.write_all(&[1]).is_err() { continue; }
                        } else {
                            let _ = control_stream.write_all(&[0]);
                            eprintln!("❌ HMAC authentication failed!");
                            continue;
                        }
                    }

                    // Pairing Handshake
                    let pairing_required = !yes && !auth_required;
                    if control_stream.write_all(&[if pairing_required { 1 } else { 0 }]).is_err() { continue; }
                    let _ = control_stream.flush();

                    if pairing_required {
                        let pairing_code = receiver::generate_pairing_code();
                        println!("\n========================================");
                        println!("🔑 Pairing Code: {}", pairing_code);
                        println!("========================================\n");

                        let mut len_bytes = [0u8; 4];
                        if control_stream.read_exact(&mut len_bytes).is_err() { continue; }
                        let len = u32::from_be_bytes(len_bytes) as usize;
                        let mut code_bytes = vec![0u8; len];
                        if control_stream.read_exact(&mut code_bytes).is_err() { continue; }
                        let received = String::from_utf8_lossy(&code_bytes).trim().to_string();

                        if received == pairing_code {
                            if control_stream.write_all(&[1]).is_err() { continue; }
                        } else {
                            let _ = control_stream.write_all(&[0]);
                            eprintln!("❌ Pairing verification failed!");
                            continue;
                        }
                    }

                    if let Err(e) = benchmark::run_speedtest_benchmark_server(&mut control_stream, &listener, options.encrypt, session_key) {
                        eprintln!("⚠️ Benchmark session completed with error: {}", e);
                    }
                    break;
                }
            }
        }
        None => {
            run_interactive_wizard();
        }
    }
}

fn pick_folder_wizard(title: &str, require_exists: bool) -> Option<PathBuf> {
    receiver::pick_folder_wizard(title, require_exists)
}

fn run_interactive_wizard() {
    println!("========================================");
    println!("🚀 NetworkCopy: Fast P2P Parallel File Copy Tool");
    println!("========================================");
    
    let selections = &["1. Sender (Upload files from this machine)", "2. Receiver (Download files to this machine)", "3. Exit"];
    let choice = dialoguer::Select::new()
        .with_prompt("Select Running Mode")
        .items(&selections[..])
        .default(0)
        .interact()
        .unwrap_or(2);
        
    match choice {
        0 => run_sender_wizard(),
        1 => run_receiver_wizard(),
        _ => {
            println!("Exiting.");
        }
    }
}

fn run_sender_wizard() {
    let src_dir = match pick_folder_wizard("Select Source Folder to Send", true) {
        Some(d) => d,
        None => {
            println!("❌ No source folder selected. Aborting.");
            return;
        }
    };
    
    // Discovery or manual IP
    println!("\n🔍 Finding Receiver on the network...");
    let mut discovered_addr = None;
    
    let use_auth_discovery = dialoguer::Confirm::new()
        .with_prompt("Do you want to use a pre-shared Auth Key for auto-discovery?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    let mut discovery_auth_key = None;
    if use_auth_discovery {
        let key: String = dialoguer::Password::new()
            .with_prompt("Enter pre-shared Auth Key")
            .interact()
            .unwrap_or_default();
        if !key.is_empty() {
            discovery_auth_key = Some(key);
        }
    }
    
    if let Ok(Some(addr)) = sender::discover_receiver(7879, discovery_auth_key.clone()) {
        println!("✨ Auto-discovered Receiver at {}!", addr);
        let connect = dialoguer::Confirm::new()
            .with_prompt("Connect to this auto-discovered Receiver?")
            .default(true)
            .interact()
            .unwrap_or(false);
        if connect {
            discovered_addr = Some(addr);
        }
    }
    
    let receiver_addr = if let Some(addr) = discovered_addr {
        addr
    } else {
        println!("\nEnter Receiver IP address or hostname:");
        let ip: String = dialoguer::Input::new()
            .with_prompt("Receiver IP/Hostname")
            .default("127.0.0.1".to_string())
            .interact_text()
            .unwrap_or_else(|_| "127.0.0.1".to_string());
            
        let port: u16 = dialoguer::Input::new()
            .with_prompt("Receiver Port")
            .default(7878)
            .interact()
            .unwrap_or(7878);
            
        format!("{}:{}", ip.trim(), port)
    };
    
    // Compression
    let comp_selections = &["Auto (suggest compression if CPU headroom exists)", "ON (Force LZ4 Compression)", "OFF (Raw Speed)"];
    let comp_choice = dialoguer::Select::new()
        .with_prompt("LZ4 Compression Mode")
        .items(&comp_selections[..])
        .default(2) // Default to OFF for maximum speed
        .interact()
        .unwrap_or(2);
    let use_compression = comp_choice == 1; // ON
    
    // Streams count
    let stream_selections = &["Auto-tune (Run speedtest to find optimal streams)", "1 Stream", "2 Streams", "4 Streams", "8 Streams", "Custom count"];
    let stream_choice = dialoguer::Select::new()
        .with_prompt("Parallel Stream Count")
        .items(&stream_selections[..])
        .default(0)
        .interact()
        .unwrap_or(0);
        
    let streams = match stream_choice {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 4,
        4 => 8,
        _ => {
            dialoguer::Input::<usize>::new()
                .with_prompt("Enter stream count (1-32)")
                .default(8)
                .interact()
                .unwrap_or(8)
        }
    };
    
    // Security & Encryption
    let sec_selections = &["Unsecure / Default (Max Speed on trusted LAN)", "HMAC-SHA256 Authenticated", "Full ChaCha20-Poly1305 Encrypted (implies Auth & Pairing)"];
    let sec_choice = dialoguer::Select::new()
        .with_prompt("Security Profile")
        .items(&sec_selections[..])
        .default(0)
        .interact()
        .unwrap_or(0);
        
    let mut auth_key = discovery_auth_key;
    let mut encrypt = false;
    
    match sec_choice {
        1 => {
            if auth_key.is_none() {
                let key: String = dialoguer::Password::new()
                    .with_prompt("Enter Auth Key / Passphrase")
                    .interact()
                    .unwrap_or_default();
                if !key.is_empty() {
                    auth_key = Some(key);
                }
            }
        }
        2 => {
            encrypt = true;
            println!("⚠️ Note: Encryption requires an Authentication Key.");
            if auth_key.is_none() {
                let key: String = dialoguer::Password::new()
                    .with_prompt("Enter Auth Key / Passphrase")
                    .interact()
                    .unwrap_or_default();
                if !key.is_empty() {
                    auth_key = Some(key);
                } else {
                    println!("❌ Encryption cannot be enabled without a passphrase. Aborting.");
                    return;
                }
            }
        }
        _ => {}
    }
    
    // Exclude / Include glob filters
    let set_filters = dialoguer::Confirm::new()
        .with_prompt("Do you want to configure Include/Exclude glob filters?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    if set_filters {
        let inc_str: String = dialoguer::Input::new()
            .with_prompt("Include glob pattern (comma-separated, e.g. *.rs,*.txt, default: all)")
            .default("".to_string())
            .interact_text()
            .unwrap_or_default();
        if !inc_str.trim().is_empty() {
            include = inc_str.split(',').map(|s| s.trim().to_string()).collect();
        }
        
        let exc_str: String = dialoguer::Input::new()
            .with_prompt("Exclude glob pattern (comma-separated, e.g. target/*,node_modules/*, default: none)")
            .default("".to_string())
            .interact_text()
            .unwrap_or_default();
        if !exc_str.trim().is_empty() {
            exclude = exc_str.split(',').map(|s| s.trim().to_string()).collect();
        }
    }
    
    // Verify existing files
    let verify_existing = dialoguer::Confirm::new()
        .with_prompt("Force CRC32 verification for skipped files (smart resume check)?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    // Dry run
    let dry_run = dialoguer::Confirm::new()
        .with_prompt("Run as a Dry Run first (shows estimation only)?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    let options = SenderOptions {
        includes: include,
        excludes: exclude,
        verify_existing,
        dry_run,
        no_discovery: false,
        auth_key: auth_key.clone(),
        control_port: 7878,
        discovery_port: 7879,
        auto_accept: false,
        pairing_code: None,
        encrypt,
    };
    
    println!("\n========================================");
    println!("🚀 Ready to start Sender session!");
    println!("📂 Source: {:?}", src_dir);
    println!("🔌 Target: {}", receiver_addr);
    println!("🔒 Authentication: {}", if auth_key.is_some() { "ON" } else { "OFF" });
    println!("🔒 Encryption: {}", if encrypt { "ON" } else { "OFF" });
    println!("🗜️ LZ4 Compression: {}", if use_compression { "ON" } else { "OFF" });
    println!("🧵 Parallel Streams: {}", if streams == 0 { "Auto-tune".to_string() } else { streams.to_string() });
    println!("========================================\n");
    
    let proceed = dialoguer::Confirm::new()
        .with_prompt("Start file transfer?")
        .default(true)
        .interact()
        .unwrap_or(false);
        
    if proceed {
        if let Err(e) = sender::run_sender(src_dir, &receiver_addr, streams, use_compression, options) {
            eprintln!("❌ Sender Error: {}", e);
        }
    } else {
        println!("Cancelled.");
    }
}

fn run_receiver_wizard() {
    let dst_dir = match pick_folder_wizard("Select Destination Folder for Downloads", false) {
        Some(d) => d,
        None => {
            println!("❌ No destination folder selected. Aborting.");
            return;
        }
    };
    
    let port: u16 = dialoguer::Input::new()
        .with_prompt("Bind TCP Port")
        .default(7878)
        .interact()
        .unwrap_or(7878);
        
    let bind_ip: String = dialoguer::Input::new()
        .with_prompt("Bind IP Address")
        .default("0.0.0.0".to_string())
        .interact_text()
        .unwrap_or_else(|_| "0.0.0.0".to_string());
        
    let discovery_port: u16 = dialoguer::Input::new()
        .with_prompt("UDP Discovery Port")
        .default(7879)
        .interact()
        .unwrap_or(7879);
        
    let loop_mode = dialoguer::Confirm::new()
        .with_prompt("Enable Loop Mode (stay active after a transfer completes)?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    let sec_selections = &["Unsecure / Default (Max Speed on trusted LAN)", "HMAC-SHA256 Authenticated", "Full ChaCha20-Poly1305 Encrypted (implies Auth)"];
    let sec_choice = dialoguer::Select::new()
        .with_prompt("Security Profile")
        .items(&sec_selections[..])
        .default(0)
        .interact()
        .unwrap_or(0);
        
    let mut auth_key = None;
    let mut encrypt = false;
    
    match sec_choice {
        1 => {
            let key: String = dialoguer::Password::new()
                .with_prompt("Enter Auth Key / Passphrase")
                .interact()
                .unwrap_or_default();
            if !key.is_empty() {
                auth_key = Some(key);
            }
        }
        2 => {
            encrypt = true;
            println!("⚠️ Note: Encryption requires an Authentication Key.");
            let key: String = dialoguer::Password::new()
                .with_prompt("Enter Auth Key / Passphrase")
                .interact()
                .unwrap_or_default();
            if !key.is_empty() {
                auth_key = Some(key);
            } else {
                println!("❌ Encryption cannot be enabled without a passphrase. Aborting.");
                return;
            }
        }
        _ => {}
    }
    
    let verify_existing = dialoguer::Confirm::new()
        .with_prompt("Force CRC32 verification for skipped files (smart resume check)?")
        .default(false)
        .interact()
        .unwrap_or(false);
        
    let listen_addr = format!("{}:{}", bind_ip.trim(), port);
    let options = ReceiverOptions {
        verify_existing,
        loop_mode,
        auth_key: auth_key.clone(),
        control_port: port,
        discovery_port,
        pairing_code: None,
        encrypt,
    };
    
    println!("\n========================================");
    println!("🚀 Ready to start Receiver listener!");
    println!("📂 Destination: {:?}", dst_dir);
    println!("🔌 Listening Address: {}", listen_addr);
    println!("🔒 Authentication: {}", if auth_key.is_some() { "ON" } else { "OFF" });
    println!("🔒 Encryption: {}", if encrypt { "ON" } else { "OFF" });
    println!("⚙️ Loop Mode: {}", if loop_mode { "ON" } else { "OFF" });
    println!("========================================\n");
    
    if let Err(e) = receiver::run_receiver(dst_dir, &listen_addr, true, options) {
        eprintln!("❌ Receiver Error: {}", e);
    }
}
