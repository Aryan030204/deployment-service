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
    combined = base64.b64decode(encrypted_text.encode("utf-8"))
    
    if len(combined) < 16:
        raise ValueError("Invalid encrypted data: too short")
        
    iv = combined[:16]
    ciphertext = combined[16:]
    
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(ciphertext) + decryptor.finalize()
    
    unpadder = padding.PKCS7(128).unpadder()
    data = unpadder.update(padded_data) + unpadder.finalize()
    
    return data.decode("utf-8")
