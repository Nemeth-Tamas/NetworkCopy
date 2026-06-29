use networkcopy::{receiver, sender};
use std::io::{self, Write};
use std::path::PathBuf;

fn print_usage() {
    println!("Usage:");
    println!("  networkcopy send <src_dir> --ip <ip> [--streams <n>] [--compress]");
    println!("  networkcopy receive <dst_dir>");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "send" => {
                let mut path = None;
                let mut ip = "127.0.0.1".to_string();
                let mut streams = 0;
                let mut compress = false;

                let mut i = 2;
                while i < args.len() {
                    match args[i].as_str() {
                        "--ip" => {
                            if i + 1 < args.len() {
                                ip = args[i + 1].clone();
                                i += 2;
                            } else {
                                eprintln!("❌ Error: Missing IP address value.");
                                return;
                            }
                        }
                        "--streams" => {
                            if i + 1 < args.len() {
                                streams = args[i + 1].parse::<usize>().unwrap_or(0);
                                i += 2;
                            } else {
                                eprintln!("❌ Error: Missing streams value.");
                                return;
                            }
                        }
                        "--compress" => {
                            compress = true;
                            i += 1;
                        }
                        p => {
                            path = Some(PathBuf::from(p));
                            i += 1;
                        }
                    }
                }

                if let Some(src_path) = path {
                    let receiver_addr = format!("{}:7878", ip);
                    println!("🚀 Running in CLI Sender mode...");
                    println!("📂 Source: {:?}", src_path);
                    println!("🔌 Target: {}", receiver_addr);
                    println!("🧵 Streams: {} (0=auto)", streams);
                    println!("🗜️ LZ4 Compression: {}", compress);
                    if let Err(e) = sender::run_sender(src_path, &receiver_addr, streams, compress) {
                        eprintln!("❌ Sender Error: {}", e);
                    }
                } else {
                    eprintln!("❌ Error: Missing source directory path.");
                    print_usage();
                }
            }
            "receive" => {
                let mut path = None;
                let mut i = 2;
                while i < args.len() {
                    path = Some(PathBuf::from(&args[i]));
                    i += 1;
                }

                if let Some(dst_path) = path {
                    println!("🚀 Running in CLI Receiver mode...");
                    println!("📂 Destination: {:?}", dst_path);
                    println!("👂 Listening on port 7878...");
                    if let Err(e) = receiver::run_receiver(dst_path, "0.0.0.0:7878") {
                        eprintln!("❌ Receiver Error: {}", e);
                    }
                } else {
                    eprintln!("❌ Error: Missing destination directory path.");
                    print_usage();
                }
            }
            _ => {
                eprintln!("❌ Error: Invalid command.");
                print_usage();
            }
        }
        return;
    }
    println!("========================================");
    println!("🚀 NetworkCopy: Fast P2P Parallel File Sender");
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

                if let Ok(Some(addr)) = sender::discover_receiver() {
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
                println!(
                    "\nStarting transfer to {} (streams: {}, compression: {})...",
                    receiver_addr, num_streams, use_compression
                );
                if let Err(e) = sender::run_sender(src_path, &receiver_addr, num_streams, use_compression) {
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
                println!("\nSStarting receiver. Listening on port 7878...");
                if let Err(e) = receiver::run_receiver(dst_path, "0.0.0.0:7878") {
                    eprintln!("❌ Receiver Error: {}", e);
                }
            } else {
                println!("❌ Folder selection cancelled.");
            }
        }
        _ => {
            println!("❌ Invalid choice. Exiting.")
        }
    }
}
