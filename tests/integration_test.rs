use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;
use networkcopy::{receiver, sender};

fn generate_dataset(src_dir: &Path) -> io::Result<()> {
    // 1. Generate 2000 small files in nested directories
    for i in 0..2000 {
        let subdir = src_dir
            .join(format!("dir_{}", i % 10))
            .join(format!("subdir_{}", (i / 10) % 10));
        fs::create_dir_all(&subdir)?;

        let file_path = subdir.join(format!("small_file_{}.txt", i));
        let mut file = File::create(file_path)?;
        let content = format!("This is small file number {} content!", i);
        file.write_all(content.as_bytes())?;
    }

    // 2. Generate 5 large files (1000 MB / ~1 GB each)
    for i in 0..5 {
        let file_path = src_dir.join(format!("large_file_{}.bin", i));
        let file = File::create(file_path)?;
        let mut writer = BufWriter::new(file);
        
        let chunk = vec![i as u8; 1024 * 1024]; // 1MB pattern chunk
        for _ in 0..1000 {
            writer.write_all(&chunk)?;
        }
        writer.flush()?;
    }

    // 3. Generate a zero-byte file
    let zero_file_path = src_dir.join("zero_byte_file.bin");
    File::create(zero_file_path)?;

    Ok(())
}

fn verify_transfer(src_dir: &Path, dst_dir: &Path) -> io::Result<()> {
    let src_files = sender::scan_directory(src_dir, &[], &[], false)?;
    
    for file_entry in src_files {
        let src_path = src_dir.join(&file_entry.rel_path);
        let dst_path = dst_dir.join(&file_entry.rel_path);
        
        assert!(dst_path.exists(), "File missing in destination: {:?}", file_entry.rel_path);
        
        let src_meta = fs::metadata(&src_path)?;
        let dst_meta = fs::metadata(&dst_path)?;
        assert_eq!(src_meta.len(), dst_meta.len(), "File size mismatch for {:?}", file_entry.rel_path);
        
        let f_src = File::open(&src_path)?;
        let f_dst = File::open(&dst_path)?;
        let mut r_src = BufReader::new(f_src);
        let mut r_dst = BufReader::new(f_dst);
        
        let mut buf_src = [0u8; 64 * 1024];
        let mut buf_dst = [0u8; 64 * 1024];
        
        loop {
            let n_src = r_src.read(&mut buf_src)?;
            let n_dst = r_dst.read(&mut buf_dst)?;
            assert_eq!(n_src, n_dst, "Read size mismatch for {:?}", file_entry.rel_path);
            if n_src == 0 {
                break;
            }
            assert_eq!(&buf_src[..n_src], &buf_dst[..n_dst], "Content mismatch for {:?}", file_entry.rel_path);
        }
    }
    Ok(())
}

#[test]
fn test_loopback_transfer() {
    let test_root = Path::new("target/test_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");

    // Clean test directories
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    println!("Creating test dataset...");
    generate_dataset(&src_dir).unwrap();

    // ---- TEST RUN 1: Full Transfer with LZ4 Disabled ----
    {
        println!("\n--- RUN 1: Full Transfer (Uncompressed) ---");
        let dst_dir_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999", false, receiver::ReceiverOptions::default()).unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, false, sender::SenderOptions { auto_accept: true, ..Default::default() }).unwrap();
        receiver_handle.join().unwrap();

        println!("Verifying Run 1 data integrity...");
        verify_transfer(&src_dir, &dst_dir).unwrap();
    }

    // ---- TEST RUN 2: Smart Resume / Skip-Existing (No modifications) ----
    {
        println!("\n--- RUN 2: Smart Resume (No modifications, should skip all) ---");
        let dst_dir_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999", false, receiver::ReceiverOptions::default()).unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, true, sender::SenderOptions { auto_accept: true, ..Default::default() }).unwrap();
        receiver_handle.join().unwrap();

        println!("Verifying Run 2 data integrity...");
        verify_transfer(&src_dir, &dst_dir).unwrap();
    }

    // ---- TEST RUN 3: Smart Resume with 1 Modified File + 1 New File ----
    {
        println!("\n--- RUN 3: Smart Resume (1 Modified + 1 New file, Compressed) ---");
        // Modify a small file
        let mod_file_path = src_dir.join("dir_0").join("subdir_0").join("small_file_0.txt");
        {
            let mut file = File::create(&mod_file_path).unwrap();
            file.write_all(b"MODIFIED CONTENT!").unwrap();
        }
        // Force update the modification time to make sure it stands out
        let new_time = filetime::FileTime::from_system_time(std::time::SystemTime::now() + Duration::from_secs(10));
        filetime::set_file_times(&mod_file_path, new_time, new_time).unwrap();

        // Add a new file
        let new_file_path = src_dir.join("new_file.txt");
        {
            let mut file = File::create(&new_file_path).unwrap();
            file.write_all(b"NEW FILE CONTENT!").unwrap();
        }

        let dst_dir_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999", false, receiver::ReceiverOptions::default()).unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, true, sender::SenderOptions { auto_accept: true, ..Default::default() }).unwrap();
        receiver_handle.join().unwrap();

        println!("Verifying Run 3 data integrity...");
        verify_transfer(&src_dir, &dst_dir).unwrap();
    }

    // Clean up on success
    println!("Cleaning up test dataset...");
    fs::remove_dir_all(test_root).unwrap();
    println!("Success!");
}

#[test]
fn test_robust_scanning_skips_inaccessible() {
    let test_root = Path::new("target/test_scan_env");
    let src_dir = test_root.join("src");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();

    let accessible_path = src_dir.join("accessible.txt");
    fs::write(&accessible_path, b"hello").unwrap();

    let inaccessible_path = src_dir.join("inaccessible.txt");
    fs::write(&inaccessible_path, b"secret").unwrap();

    // Lock the file on Windows or remove permissions on Unix
    #[cfg(windows)]
    let _keep_locked = {
        use std::os::windows::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .share_mode(0) // FILE_SHARE_NONE (exclusive lock)
            .open(&inaccessible_path)
    };
        
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = fs::metadata(&inaccessible_path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o000);
            let _ = fs::set_permissions(&inaccessible_path, perms);
        }
    }

    let can_still_read = std::fs::File::open(&inaccessible_path).is_ok();

    // Scan directory
    let files = sender::scan_directory(&src_dir, &[], &[], false).unwrap();
    
    // Verify that inaccessible.txt was skipped (if it was actually inaccessible)
    let scanned_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
    assert!(scanned_names.contains(&"accessible.txt".to_string()));
    if !can_still_read {
        assert!(!scanned_names.contains(&"inaccessible.txt".to_string()));
    }

    // Cleanup permissions on Unix so we can delete the directory
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = fs::metadata(&inaccessible_path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            let _ = fs::set_permissions(&inaccessible_path, perms);
        }
    }
    
    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_path_sanitization_rules() {
    use networkcopy::protocol::sanitize_rel_path;

    // Safe relative paths
    assert_eq!(sanitize_rel_path("foo.txt"), Some("foo.txt".to_string()));
    assert_eq!(sanitize_rel_path("foo/bar.txt"), Some("foo/bar.txt".to_string()));

    // Directory traversals (rejected)
    assert_eq!(sanitize_rel_path("../foo.txt"), None);
    assert_eq!(sanitize_rel_path("foo/../bar.txt"), None);

    // Absolute and root paths (rejected)
    assert_eq!(sanitize_rel_path("/foo.txt"), None);
    assert_eq!(sanitize_rel_path("\\foo.txt"), None);
    assert_eq!(sanitize_rel_path("C:\\foo.txt"), None);

    // Windows reserved device names (rejected)
    assert_eq!(sanitize_rel_path("CON"), None);
    assert_eq!(sanitize_rel_path("com1.txt"), None);
    assert_eq!(sanitize_rel_path("foo/PRN/bar"), None);

    // Trailing dots and spaces (rejected)
    assert_eq!(sanitize_rel_path("foo. "), None);
    assert_eq!(sanitize_rel_path("bar/baz."), None);
}

#[test]
fn test_glob_filtering() {
    let test_root = Path::new("target/test_glob_env");
    let src_dir = test_root.join("src");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();

    fs::write(src_dir.join("a.rs"), b"a").unwrap();
    fs::write(src_dir.join("b.txt"), b"b").unwrap();
    fs::create_dir_all(src_dir.join("target")).unwrap();
    fs::write(src_dir.join("target").join("c.rs"), b"c").unwrap();

    // Test 1: Exclude "target/*"
    let files = sender::scan_directory(&src_dir, &[], &["target/*".to_string()], false).unwrap();
    let file_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
    assert!(file_names.contains(&"a.rs".to_string()));
    assert!(file_names.contains(&"b.txt".to_string()));
    assert!(!file_names.contains(&"target/c.rs".to_string()));

    // Test 2: Include "*.rs"
    let files = sender::scan_directory(&src_dir, &["*.rs".to_string()], &[], false).unwrap();
    let file_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
    assert!(file_names.contains(&"a.rs".to_string()));
    assert!(!file_names.contains(&"b.txt".to_string()));

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_hmac_auth() {
    let test_root = Path::new("target/test_auth_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    fs::write(src_dir.join("data.txt"), b"secure data").unwrap();

    // Case 1: Receiver expects auth with key "secret", Sender provides matching key.
    {
        let dst_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            let options = receiver::ReceiverOptions {
                auth_key: Some("secret".to_string()),
                control_port: 9991,
                discovery_port: 9992,
                ..Default::default()
            };
            receiver::run_receiver(dst_clone, "127.0.0.1:9991", false, options)
        });

        thread::sleep(Duration::from_millis(200));

        let options = sender::SenderOptions {
            auth_key: Some("secret".to_string()),
            control_port: 9991,
            discovery_port: 9992,
            auto_accept: true,
            ..Default::default()
        };
        let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9991", 1, false, options);
        assert!(result.is_ok(), "Transfer with matching auth keys should succeed");
        let _ = receiver_handle.join();
    }

    // Case 2: Receiver expects auth with key "secret", Sender provides incorrect key "wrong".
    {
        let dst_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            let options = receiver::ReceiverOptions {
                auth_key: Some("secret".to_string()),
                control_port: 9993,
                discovery_port: 9994,
                ..Default::default()
            };
            let _ = receiver::run_receiver(dst_clone, "127.0.0.1:9993", false, options);
        });

        thread::sleep(Duration::from_millis(200));

        let options = sender::SenderOptions {
            auth_key: Some("wrong".to_string()),
            control_port: 9993,
            discovery_port: 9994,
            auto_accept: true,
            ..Default::default()
        };
        let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9993", 1, false, options);
        assert!(result.is_err(), "Transfer with mismatching auth keys must fail");
        let _ = receiver_handle.join();
    }

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_dry_run() {
    let test_root = Path::new("target/test_dry_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    fs::write(src_dir.join("file.txt"), b"should not transfer").unwrap();

    let dst_clone = dst_dir.clone();
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9995,
            discovery_port: 9996,
            ..Default::default()
        };
        receiver::run_receiver(dst_clone, "127.0.0.1:9995", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        dry_run: true,
        control_port: 9995,
        discovery_port: 9996,
        auto_accept: true,
        ..Default::default()
    };
    let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9995", 1, false, options);
    assert!(result.is_ok(), "Dry-run flow should complete successfully");
    let _ = receiver_handle.join();

    // Verify that the file was NOT created in the destination directory
    assert!(!dst_dir.join("file.txt").exists(), "Dry-run should not create files");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_pairing_handshake() {
    let test_root = Path::new("target/test_pairing_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    fs::write(src_dir.join("file.txt"), b"some file").unwrap();

    // Case 1: Matching pairing code.
    {
        let dst_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            let options = receiver::ReceiverOptions {
                control_port: 9981,
                discovery_port: 9982,
                pairing_code: Some("4321".to_string()),
                ..Default::default()
            };
            // Set is_interactive = true so pairing is enforced
            receiver::run_receiver(dst_clone, "127.0.0.1:9981", true, options)
        });

        thread::sleep(Duration::from_millis(200));

        let options = sender::SenderOptions {
            control_port: 9981,
            discovery_port: 9982,
            pairing_code: Some("4321".to_string()),
            ..Default::default()
        };
        let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9981", 1, false, options);
        assert!(result.is_ok(), "Pairing handshake with matching code should succeed");
        let _ = receiver_handle.join();
    }

    // Case 2: Mismatching pairing code.
    {
        let dst_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            let options = receiver::ReceiverOptions {
                control_port: 9983,
                discovery_port: 9984,
                pairing_code: Some("4321".to_string()),
                ..Default::default()
            };
            let _ = receiver::run_receiver(dst_clone, "127.0.0.1:9983", true, options);
        });

        thread::sleep(Duration::from_millis(200));

        let options = sender::SenderOptions {
            control_port: 9983,
            discovery_port: 9984,
            pairing_code: Some("wrong".to_string()),
            ..Default::default()
        };
        let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9983", 1, false, options);
        assert!(result.is_err(), "Pairing handshake with mismatching code must fail");
        let _ = receiver_handle.join();
    }

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_partial_file_resume() {
    let test_root = Path::new("target/test_resume_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    // 1. Create a 100KB random source file
    let file_size = 100 * 1024;
    let mut file_content = vec![0u8; file_size];
    for (i, byte) in file_content.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let src_file_path = src_dir.join("file.txt");
    fs::write(&src_file_path, &file_content).unwrap();

    // 2. Pre-create a partial 40KB temp file in dst
    let partial_size = 40 * 1024;
    let dst_tmp_path = dst_dir.join("file.txt.networkcopy-tmp");
    fs::write(&dst_tmp_path, &file_content[..partial_size]).unwrap();

    // 3. Start receiver and sender
    let dst_clone = dst_dir.clone();
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9971,
            discovery_port: 9972,
            ..Default::default()
        };
        receiver::run_receiver(dst_clone, "127.0.0.1:9971", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        control_port: 9971,
        discovery_port: 9972,
        auto_accept: true,
        ..Default::default()
    };
    let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9971", 1, false, options);
    assert!(result.is_ok(), "Resumed file transfer should succeed");
    let _ = receiver_handle.join();

    // 4. Verify that final file was completed and matches source
    let dst_file_path = dst_dir.join("file.txt");
    assert!(dst_file_path.exists(), "Final file should exist");
    let dst_content = fs::read(&dst_file_path).unwrap();
    assert_eq!(dst_content, file_content, "Resumed file content should match source");
    assert!(!dst_tmp_path.exists(), "Temp file should be deleted on success");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
#[cfg(unix)]
fn test_unix_permissions_preservation() {
    use std::os::unix::fs::PermissionsExt;
    
    let test_root = Path::new("/tmp/test_perms_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    // Create file and set mode to 0o755 (executable)
    let src_file_path = src_dir.join("script.sh");
    fs::write(&src_file_path, b"#!/bin/sh\necho 'hello'").unwrap();
    let mut perms = fs::metadata(&src_file_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&src_file_path, perms).unwrap();

    let dst_clone = dst_dir.clone();
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9961,
            discovery_port: 9962,
            ..Default::default()
        };
        receiver::run_receiver(dst_clone, "127.0.0.1:9961", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        control_port: 9961,
        discovery_port: 9962,
        auto_accept: true,
        ..Default::default()
    };
    sender::run_sender(src_dir.clone(), "127.0.0.1:9961", 1, false, options).unwrap();
    let _ = receiver_handle.join();

    // Verify permissions at destination
    let dst_file_path = dst_dir.join("script.sh");
    let dst_perms = fs::metadata(&dst_file_path).unwrap().permissions();
    assert_eq!(dst_perms.mode() & 0o777, 0o755, "Permissions should be preserved as 0o755");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_json_preset_execution() {
    let test_root = Path::new("target/test_preset_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    // Create a source file
    let src_file_path = src_dir.join("preset_file.txt");
    fs::write(&src_file_path, b"Preset data transfer test!").unwrap();

    // Create JSON preset files for both send and receive jobs
    let send_preset_path = test_root.join("send_preset.json");
    let send_json = format!(
        r#"{{
            "role": "send",
            "path": {:?},
            "ip": "127.0.0.1",
            "port": 9951,
            "streams": 1,
            "compress": false,
            "no_discovery": true,
            "yes": true
        }}"#,
        src_dir.to_str().unwrap().replace('\\', "/")
    );
    fs::write(&send_preset_path, send_json).unwrap();

    let recv_preset_path = test_root.join("recv_preset.json");
    let recv_json = format!(
        r#"{{
            "role": "receive",
            "path": {:?},
            "port": 9951,
            "yes": true
        }}"#,
        dst_dir.to_str().unwrap().replace('\\', "/")
    );
    fs::write(&recv_preset_path, recv_json).unwrap();

    // Run receiver preset in a background thread
    let recv_preset_clone = recv_preset_path.clone();
    let receiver_handle = thread::spawn(move || {
        networkcopy::preset::run_preset(recv_preset_clone)
    });

    thread::sleep(Duration::from_millis(200));

    // Run sender preset in the main thread
    let result = networkcopy::preset::run_preset(send_preset_path);
    assert!(result.is_ok(), "Sender preset job should complete successfully");

    let _ = receiver_handle.join().unwrap();

    // Verify file transferred successfully
    let dst_file_path = dst_dir.join("preset_file.txt");
    assert!(dst_file_path.exists(), "Preset transferred file should exist");
    let content = fs::read_to_string(&dst_file_path).unwrap();
    assert_eq!(content, "Preset data transfer test!");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_benchmark_execution() {
    // Run benchmark server in a background thread
    let receiver_handle = thread::spawn(move || {
        let bind_addr = "127.0.0.1:9941";
        let listener = std::net::TcpListener::bind(bind_addr).unwrap();
        loop {
            let (mut control_stream, _) = listener.accept().unwrap();
            let mut magic = [0u8; 4];
            control_stream.read_exact(&mut magic).unwrap();
            let mut mode = [0u8; 1];
            control_stream.read_exact(&mut mode).unwrap();
            let mut version = [0u8; 1];
            control_stream.read_exact(&mut version).unwrap();
            let mut encrypt = [0u8; 1];
            control_stream.read_exact(&mut encrypt).unwrap();
            // Auth Handshake (disabled)
            control_stream.write_all(&[0]).unwrap();
            // Pairing Handshake (disabled because we simulate auto-accept)
            control_stream.write_all(&[0]).unwrap();

            let mut maybe_stream = networkcopy::encrypted_stream::MaybeEncryptedStream::Raw(control_stream);
            let res = networkcopy::benchmark::run_speedtest_benchmark_server(&mut maybe_stream, &listener, false, [0u8; 32]);
            assert!(res.is_ok(), "Benchmark server should finish successfully");
            break;
        }
    });

    thread::sleep(Duration::from_millis(200));

    // Run benchmark client in the main thread
    let options = sender::SenderOptions {
        control_port: 9941,
        auto_accept: true,
        ..Default::default()
    };
    let result = sender::run_benchmark_sender("127.0.0.1:9941", 2, 1, options);
    assert!(result.is_ok(), "Benchmark client should run and complete successfully");

    let _ = receiver_handle.join().unwrap();
}

#[test]
fn test_encrypted_file_transfer() {
    let test_root = Path::new("target/test_encrypted_transfer");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    fs::write(src_dir.join("confidential.txt"), b"highly classified document data").unwrap();

    let dst_clone = dst_dir.clone();
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9931,
            discovery_port: 9932,
            auth_key: Some("secure_passphrase".to_string()),
            encrypt: true,
            ..Default::default()
        };
        receiver::run_receiver(dst_clone, "127.0.0.1:9931", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        control_port: 9931,
        discovery_port: 9932,
        auth_key: Some("secure_passphrase".to_string()),
        encrypt: true,
        auto_accept: true,
        ..Default::default()
    };
    let result = sender::run_sender(src_dir.clone(), "127.0.0.1:9931", 1, false, options);
    assert!(result.is_ok(), "Encrypted file transfer should succeed");
    
    let _ = receiver_handle.join().unwrap();

    // Verify file content is intact
    let dst_file_path = dst_dir.join("confidential.txt");
    assert!(dst_file_path.exists());
    let content = fs::read_to_string(dst_file_path).unwrap();
    assert_eq!(content, "highly classified document data");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_encrypted_benchmark_execution() {
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9921,
            discovery_port: 9922,
            auth_key: Some("benchmark_passphrase".to_string()),
            encrypt: true,
            ..Default::default()
        };
        receiver::run_receiver(Path::new("target").to_path_buf(), "127.0.0.1:9921", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        control_port: 9921,
        discovery_port: 9922,
        auth_key: Some("benchmark_passphrase".to_string()),
        encrypt: true,
        auto_accept: true,
        ..Default::default()
    };
    let result = sender::run_benchmark_sender("127.0.0.1:9921", 2, 1, options);
    assert!(result.is_ok(), "Encrypted benchmark should run successfully");

    let _ = receiver_handle.join().unwrap();
}

#[test]
fn test_symlink_skipping() {
    let test_root = Path::new("target/test_symlink_env");
    let src_dir = test_root.join("src");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    
    let file_path = src_dir.join("real.txt");
    fs::write(&file_path, b"real file content").unwrap();
    
    let link_path = src_dir.join("link.txt");
    
    // Create symlink depending on target OS
    let symlink_created = {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&file_path, &link_path).is_ok()
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(&file_path, &link_path).is_ok()
        }
    };
    
    let files = sender::scan_directory(&src_dir, &[], &[], false).unwrap();
    let file_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
    
    // "real.txt" must be present.
    assert!(file_names.contains(&"real.txt".to_string()));
    
    // If symlink was successfully created, it must NOT be in the scanned list!
    if symlink_created {
        assert!(!file_names.contains(&"link.txt".to_string()));
    }
    
    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_cross_platform_path_handling() {
    let test_root = Path::new("target/test_cross_platform_paths");
    let src_dir = test_root.join("src");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();

    // Create deep directories, files with spaces, and Unicode file names
    let deep_dir = src_dir.join("deep").join("folder name with spaces").join("nested");
    fs::create_dir_all(&deep_dir).unwrap();
    
    let file_unicode = deep_dir.join("üñîçøðé.txt");
    fs::write(&file_unicode, b"unicode content").unwrap();

    let files = sender::scan_directory(&src_dir, &[], &[], false).unwrap();
    let file_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();

    // Verify spaces and Unicode paths are scanned correctly and backslashes normalized to forward slashes
    assert_eq!(files.len(), 1);
    assert_eq!(file_names[0], "deep/folder name with spaces/nested/üñîçøðé.txt");

    let _ = fs::remove_dir_all(test_root);
}

#[test]
fn test_full_file_resume_integrity() {
    let test_root = Path::new("target/test_resume_integrity_env");
    let src_dir = test_root.join("src");
    let dst_dir = test_root.join("dst");
    if test_root.exists() {
        let _ = fs::remove_dir_all(test_root);
    }
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dst_dir).unwrap();

    // 1. Create a 10KB random source file
    let file_size = 10 * 1024;
    let mut file_content = vec![0u8; file_size];
    for (i, byte) in file_content.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let src_file_path = src_dir.join("file.txt");
    fs::write(&src_file_path, &file_content).unwrap();

    // 2. Pre-create a partial 4KB temp file in dst with CORRUPTED prefix (first 2KB different)
    let partial_size = 4 * 1024;
    let mut corrupted_partial = file_content[..partial_size].to_vec();
    for i in 0..2048 {
        corrupted_partial[i] = corrupted_partial[i].wrapping_add(1);
    }
    let dst_tmp_path = dst_dir.join("file.txt.networkcopy-tmp");
    fs::write(&dst_tmp_path, &corrupted_partial).unwrap();

    // 3. Start receiver and sender
    let dst_clone = dst_dir.clone();
    let receiver_handle = thread::spawn(move || {
        let options = receiver::ReceiverOptions {
            control_port: 9965,
            discovery_port: 9966,
            ..Default::default()
        };
        receiver::run_receiver(dst_clone, "127.0.0.1:9965", false, options)
    });

    thread::sleep(Duration::from_millis(200));

    let options = sender::SenderOptions {
        control_port: 9965,
        discovery_port: 9966,
        auto_accept: true,
        ..Default::default()
    };
    
    // The transfer completes on sender side, but the receiver must fail verification!
    let _ = sender::run_sender(src_dir.clone(), "127.0.0.1:9965", 1, false, options);
    
    let receiver_res = receiver_handle.join().unwrap();
    assert!(receiver_res.is_err(), "Receiver must return error on verification failure");

    // 4. Verify that final file was NOT created and corrupted temp file was deleted
    let dst_file_path = dst_dir.join("file.txt");
    assert!(!dst_file_path.exists(), "Final file should not exist");
    assert!(!dst_tmp_path.exists(), "Corrupted temp file should be deleted on verification failure");

    let _ = fs::remove_dir_all(test_root);
}
