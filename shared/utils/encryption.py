import base64
import os
import hashlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding

def _get_key() -> bytes:
    """Derive a 32-byte key from PASSWORD_AES_KEY using SHA-256."""
    password = os.getenv("PASSWORD_AES_KEY")
    if not password:
        raise ValueError("PASSWORD_AES_KEY environment variable is not set")
    return hashlib.sha256(password.encode("utf-8")).digest()

def encrypt(text: str) -> str:
    """
    Encrypt text using AES-256-CBC.
    Prepends a 16-byte random IV, then returns base64 string.
    """
    if not text:
        return ""
        
    key = _get_key()
    iv = os.urandom(16)
    
    # Pad plaintext to block size (16 bytes for AES)
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(text.encode("utf-8")) + padder.finalize()
    
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()
    
    # Combine IV and ciphertext, then base64 encode
    combined = iv + ciphertext
    return base64.b64encode(combined).decode("utf-8")

def decrypt(encrypted_text: str) -> str:
    """
    Decrypt text using AES-256-CBC.
    Expects base64 string with 16-byte IV prefix.
    """
    if not encrypted_text:
        return ""
        
    key = _get_key()
    
    if ":" in encrypted_text:
        iv_b64, ciphertext_b64 = encrypted_text.split(":", 1)
        iv_bytes = base64.b64decode(iv_b64.encode("utf-8"))
        ciphertext_bytes = base64.b64decode(ciphertext_b64.encode("utf-8"))
        combined = iv_bytes + ciphertext_bytes
    else:
        combined = base64.b64decode(encrypted_text.encode("utf-8"))
    
    if len(combined) < 16:
        raise ValueError("Invalid encrypted data: too short")
        
    iv = combined[:16]
    ciphertext = combined[16:]
    
    # 1. Try standard SHA-256 derived key
    try:
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        return (unpadder.update(padded_data) + unpadder.finalize()).decode("utf-8")
    except Exception:
        # 2. Fallback to raw NULL padded key (API node formats)
        raw_pass = os.getenv("PASSWORD_AES_KEY") or ""
        key_pad = raw_pass.encode("utf-8").ljust(32, b'\0')[:32]
        
        cipher = Cipher(algorithms.AES(key_pad), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        return (unpadder.update(padded_data) + unpadder.finalize()).decode("utf-8")
