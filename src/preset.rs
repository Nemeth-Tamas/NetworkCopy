use crate::{receiver::{self, ReceiverOptions}, sender::{self, SenderOptions}};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PresetConfig {
    pub role: String, // "send" or "receive"
    pub path: PathBuf,
    pub ip: Option<String>,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub streams: usize,
    #[serde(default)]
    pub compress: bool,
    #[serde(default)]
    pub no_discovery: bool,
    #[serde(default)]
    pub yes: bool,
    #[serde(default)]
    pub verify_existing: bool,
    pub auth: Option<String>,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "default_discovery_port")]
    pub discovery_port: u16,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub loop_mode: bool,
}

fn default_port() -> u16 {
    7878
}
fn default_discovery_port() -> u16 {
    7879
}
fn default_bind() -> String {
    "0.0.0.0".to_string()
}

pub fn run_preset(preset_path: PathBuf) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let content = std::fs::read_to_string(preset_path)?;
    let config: PresetConfig = serde_json::from_str(&content)?;

    match config.role.to_lowercase().as_str() {
        "send" => {
            let mut target_addr = None;
            if !config.no_discovery && config.ip.is_none() {
                if let Ok(Some(discovered)) = sender::discover_receiver(config.discovery_port, config.auth.clone()) {
                    println!("✨ Auto-discovered Receiver at {}!", discovered);
                    target_addr = Some(discovered);
                }
            }

            let receiver_addr = if let Some(addr) = target_addr {
                addr
            } else {
                let ip_str = config.ip.unwrap_or_else(|| "127.0.0.1".to_string());
                format!("{}:{}", ip_str, config.port)
            };

            let options = SenderOptions {
                includes: config.include,
                excludes: config.exclude,
                verify_existing: config.verify_existing,
                dry_run: config.dry_run,
                no_discovery: config.no_discovery,
                auth_key: config.auth,
                control_port: config.port,
                discovery_port: config.discovery_port,
                auto_accept: config.yes,
                pairing_code: None,
            };

            println!("🚀 Running Preset Job (Sender)...");
            println!("📂 Source: {:?}", config.path);
            println!("🔌 Target: {}", receiver_addr);
            println!("🗜️ LZ4 Compression: {}", config.compress);
            sender::run_sender(config.path, &receiver_addr, config.streams, config.compress, options)?;
        }
        "receive" => {
            let listen_addr = format!("{}:{}", config.bind, config.port);
            let options = ReceiverOptions {
                verify_existing: config.verify_existing,
                loop_mode: config.loop_mode,
                auth_key: config.auth,
                control_port: config.port,
                discovery_port: config.discovery_port,
                pairing_code: None,
            };

            println!("🚀 Running Preset Job (Receiver)...");
            println!("📂 Destination: {:?}", config.path);
            println!("👂 Listening on {}...", listen_addr);
            receiver::run_receiver(config.path, &listen_addr, !config.yes, options)?;
        }
        other => {
            return Err(format!(
                "Unknown role: '{}' in preset config. Expected 'send' or 'receive'.",
                other
            )
            .into());
        }
    }
    Ok(())
}
