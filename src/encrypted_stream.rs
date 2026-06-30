use chacha20poly1305::{aead::{Aead, KeyInit}, ChaCha20Poly1305, Nonce};
use std::io::{Read, Write};

pub struct EncryptedStream<S: Read + Write> {
    inner: S,
    cipher: ChaCha20Poly1305,
    stream_idx: u32,
    write_frame_counter: u64,
    read_frame_counter: u64,
    read_buffer: Vec<u8>,
    read_pos: usize,
}

impl<S: Read + Write> EncryptedStream<S> {
    pub fn new(inner: S, key: [u8; 32], stream_idx: u32) -> Self {
        let cipher = ChaCha20Poly1305::new_from_slice(&key).unwrap();
        Self {
            inner,
            cipher,
            stream_idx,
            write_frame_counter: 0,
            read_frame_counter: 0,
            read_buffer: Vec::new(),
            read_pos: 0,
        }
    }
}

impl<S: Read + Write> Read for EncryptedStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // 1. If we have remaining bytes in our read buffer, serve them first
        if self.read_pos < self.read_buffer.len() {
            let available = self.read_buffer.len() - self.read_pos;
            let to_copy = std::cmp::min(buf.len(), available);
            buf[..to_copy].copy_from_slice(&self.read_buffer[self.read_pos..self.read_pos + to_copy]);
            self.read_pos += to_copy;
            return Ok(to_copy);
        }

        // 2. Clear buffer and read a new frame
        self.read_buffer.clear();
        self.read_pos = 0;

        // Read 4-byte payload length
        let mut len_bytes = [0u8; 4];
        match self.inner.read_exact(&mut len_bytes) {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(0); // Clean EOF
            }
            Err(e) => return Err(e),
        }
        let payload_len = u32::from_be_bytes(len_bytes) as usize;

        if payload_len < 16 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid encrypted frame size",
            ));
        }

        // Read the entire payload (tag + ciphertext)
        let mut payload = vec![0u8; payload_len];
        self.inner.read_exact(&mut payload)?;

        // Derive decrypt nonce
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..4].copy_from_slice(&self.stream_idx.to_be_bytes());
        nonce_bytes[4..].copy_from_slice(&self.read_frame_counter.to_be_bytes());
        self.read_frame_counter += 1;
        
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Decrypt
        match self.cipher.decrypt(nonce, payload.as_slice()) {
            Ok(plaintext) => {
                self.read_buffer = plaintext;
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Decryption / integrity check failed",
                ));
            }
        }

        // Serve decrypted data
        let to_copy = std::cmp::min(buf.len(), self.read_buffer.len());
        buf[..to_copy].copy_from_slice(&self.read_buffer[..to_copy]);
        self.read_pos = to_copy;
        Ok(to_copy)
    }
}

impl<S: Read + Write> Write for EncryptedStream<S> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Derive encrypt nonce
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..4].copy_from_slice(&self.stream_idx.to_be_bytes());
        nonce_bytes[4..].copy_from_slice(&self.write_frame_counter.to_be_bytes());
        self.write_frame_counter += 1;
        
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Encrypt the plaintext slice
        match self.cipher.encrypt(nonce, buf) {
            Ok(ciphertext) => {
                let payload_len = ciphertext.len() as u32;
                self.inner.write_all(&payload_len.to_be_bytes())?;
                self.inner.write_all(&ciphertext)?;
                Ok(buf.len()) // We successfully processed the entire input buffer
            }
            Err(_) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Encryption failed",
                ))
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub enum MaybeEncryptedStream<S: Read + Write> {
    Raw(S),
    Encrypted(EncryptedStream<S>),
}

impl<S: Read + Write> Read for MaybeEncryptedStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            MaybeEncryptedStream::Raw(s) => s.read(buf),
            MaybeEncryptedStream::Encrypted(s) => s.read(buf),
        }
    }
}

impl<S: Read + Write> Write for MaybeEncryptedStream<S> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            MaybeEncryptedStream::Raw(s) => s.write(buf),
            MaybeEncryptedStream::Encrypted(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            MaybeEncryptedStream::Raw(s) => s.flush(),
            MaybeEncryptedStream::Encrypted(s) => s.flush(),
        }
    }
}
