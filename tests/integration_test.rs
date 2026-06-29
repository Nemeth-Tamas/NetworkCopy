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
    let src_files = sender::scan_directory(src_dir)?;
    
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
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999").unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, false).unwrap();
        receiver_handle.join().unwrap();

        println!("Verifying Run 1 data integrity...");
        verify_transfer(&src_dir, &dst_dir).unwrap();
    }

    // ---- TEST RUN 2: Smart Resume / Skip-Existing (No modifications) ----
    {
        println!("\n--- RUN 2: Smart Resume (No modifications, should skip all) ---");
        let dst_dir_clone = dst_dir.clone();
        let receiver_handle = thread::spawn(move || {
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999").unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, true).unwrap();
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
            receiver::run_receiver(dst_dir_clone, "127.0.0.1:9999").unwrap();
        });

        thread::sleep(Duration::from_millis(200));

        sender::run_sender(src_dir.clone(), "127.0.0.1:9999", 0, true).unwrap();
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

    // Scan directory
    let files = sender::scan_directory(&src_dir).unwrap();
    
    // Verify that inaccessible.txt was skipped and accessible.txt was scanned!
    let scanned_names: Vec<String> = files.iter().map(|f| f.rel_path.clone()).collect();
    assert!(scanned_names.contains(&"accessible.txt".to_string()));
    assert!(!scanned_names.contains(&"inaccessible.txt".to_string()));

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
