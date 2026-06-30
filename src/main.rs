use clap::{Parser, Subcommand};
use networkcopy::{receiver::{self, ReceiverOptions}, sender::{self, SenderOptions}, benchmark, preset::run_preset};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "networkcopy", version = "1.1.0", about = "Fast P2P Parallel File Copy Tool")]
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
        }) => {
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
        }) => {
            let listen_addr = format!("{}:{}", bind, port);
            let options = ReceiverOptions {
                verify_existing,
                loop_mode,
                auth_key: auth,
                control_port: port,
                discovery_port,
                pairing_code: None,
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
        }) => {
            use std::io::Read;
            if let Some(target_ip) = ip {
                let receiver_addr = format!("{}:{}", target_ip, port);
                let options = SenderOptions {
                    auth_key: auth,
                    control_port: port,
                    auto_accept: yes,
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

                    if let Err(e) = benchmark::run_speedtest_benchmark_server(&mut control_stream, &listener) {
                        eprintln!("⚠️ Benchmark session completed with error: {}", e);
                    }
                    break;
                }
            }
        }
        None => {
            // Interactive mode fallback
            println!("========================================");
            println!("🚀 NetworkCopy: Fast P2P Parallel File Copy Tool");
            println!("========================================");
            println!("Select running mode:");
            println!("1. Sender (Upload files from this machine)");
            println!("2. Receiver (Download files to this machine)");
            print!("> ");
            io::stdout().flush().unwrap();

            let mut choice = String::new();
            io::stdin().read_line(&mut choice).unwrap();
            let choice = choice.trim();

            match choice {
                "1" => {
                    println!("\n📂 Opening folder dialog to select source directory...");
                    if let Some(src_path) = rfd::FileDialog::new()
                        .set_title("Select Source Folder to Send")
                        .pick_folder()
                    {
                        println!("Selected Source: {:?}", src_path);

                        let mut receiver_ip = String::new();
                        let mut discovered = false;

                        if let Ok(Some(addr)) = sender::discover_receiver(7879, None) {
                            println!("✨ Auto-discovered Receiver at {}!", addr);
                            print!("Connect to this Receiver? (Y/n): ");
                            io::stdout().flush().unwrap();
                            let mut ans = String::new();
                            io::stdin().read_line(&mut ans).unwrap();
                            let ans = ans.trim().to_lowercase();
                            if ans.is_empty() || ans.starts_with('y') {
                                receiver_ip = addr.split(':').next().unwrap_or("127.0.0.1").to_string();
                                discovered = true;
                            }
                        }

                        if !discovered {
                            print!("\nEnter Receiver IP address (default: 127.0.0.1): ");
                            io::stdout().flush().unwrap();
                            let mut ip = String::new();
                            io::stdin().read_line(&mut ip).unwrap();
                            let ip_trimmed = ip.trim();
                            receiver_ip = if ip_trimmed.is_empty() {
                                "127.0.0.1".to_string()
                            } else {
                                ip_trimmed.to_string()
                            };
                        }

                        print!("Enter number of parallel streams (0 for auto-tuning speedtest, default: 0): ");
                        io::stdout().flush().unwrap();
                        let mut streams_str = String::new();
                        io::stdin().read_line(&mut streams_str).unwrap();
                        let num_streams = streams_str.trim().parse::<usize>().unwrap_or(0);

                        print!("Enable LZ4 Compression? (y/N): ");
                        io::stdout().flush().unwrap();
                        let mut comp_str = String::new();
                        io::stdin().read_line(&mut comp_str).unwrap();
                        let use_compression = comp_str.trim().to_lowercase().starts_with('y');

                        let receiver_addr = format!("{}:7878", receiver_ip);
                        let options = SenderOptions {
                            control_port: 7878,
                            discovery_port: 7879,
                            ..Default::default()
                        };

                        println!(
                            "\nStarting transfer to {} (streams: {}, compression: {})...",
                            receiver_addr, num_streams, use_compression
                        );
                        if let Err(e) = sender::run_sender(src_path, &receiver_addr, num_streams, use_compression, options) {
                            eprintln!("❌ Sender Error: {}", e);
                        }
                    } else {
                        println!("❌ Folder selection cancelled.");
                    }
                }
                "2" => {
                    println!("\n📂 Opening folder dialog to select destination directory...");
                    if let Some(dst_path) = rfd::FileDialog::new()
                        .set_title("Select Destination Folder to Save")
                        .pick_folder()
                    {
                        println!("Selected Destination: {:?}", dst_path);
                        println!("\nStarting receiver. Listening on port 7878...");
                        
                        let options = ReceiverOptions {
                            control_port: 7878,
                            discovery_port: 7879,
                            ..Default::default()
                        };

                        if let Err(e) = receiver::run_receiver(dst_path, "0.0.0.0:7878", true, options) {
                            eprintln!("❌ Receiver Error: {}", e);
                        }
                    } else {
                        println!("❌ Folder selection cancelled.");
                    }
                }
                _ => {
                    println!("❌ Invalid choice. Exiting.");
                }
            }
        }
    }
}
