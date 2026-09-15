use std::{fs, path::Path};

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::Argon2;
use uuid::Uuid;
use zeroize::Zeroize;

const SALT_FILE: &str = "vault-salt-v1";

pub struct Ciphertext {
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// Encrypts the portable headless vault with a user-supplied passphrase.
/// OS keychain backends can replace this boundary without changing storage callers.
pub fn encrypt(data_dir: &Path, plaintext: &[u8]) -> Result<Ciphertext, String> {
    let mut key = derive_key(data_dir)?;
    let nonce_uuid = Uuid::new_v4();
    let nonce = nonce_uuid.as_bytes()[..12].to_vec();
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| "cannot initialize vault cipher")?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(|_| "cannot encrypt secret")?;
    key.zeroize();
    Ok(Ciphertext { nonce, ciphertext })
}

pub fn decrypt(data_dir: &Path, nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, String> {
    if nonce.len() != 12 {
        return Err("stored secret has an invalid vault nonce".to_string());
    }
    let mut key = derive_key(data_dir)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|_| "cannot initialize vault cipher")?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| "cannot decrypt secret; check KAKUNE_VAULT_PASSPHRASE")?;
    key.zeroize();
    Ok(plaintext)
}

fn derive_key(data_dir: &Path) -> Result<[u8; 32], String> {
    let passphrase = std::env::var("KAKUNE_VAULT_PASSPHRASE")
        .map_err(|_| "vault is locked; set KAKUNE_VAULT_PASSPHRASE".to_string())?;
    if passphrase.is_empty() {
        return Err("vault is locked; KAKUNE_VAULT_PASSPHRASE must not be empty".to_string());
    }
    let salt = load_or_create_salt(data_dir)?;
    let mut key = [0_u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), &salt, &mut key)
        .map_err(|error| format!("cannot derive vault key: {error}"))?;
    Ok(key)
}

fn load_or_create_salt(data_dir: &Path) -> Result<[u8; 16], String> {
    let path = data_dir.join(SALT_FILE);
    if path.exists() {
        let bytes = fs::read(&path).map_err(|error| format!("cannot read vault salt: {error}"))?;
        return bytes
            .try_into()
            .map_err(|_| "stored vault salt is invalid".to_string());
    }
    let salt = *Uuid::new_v4().as_bytes();
    fs::write(&path, salt).map_err(|error| format!("cannot create vault salt: {error}"))?;
    Ok(salt)
}
