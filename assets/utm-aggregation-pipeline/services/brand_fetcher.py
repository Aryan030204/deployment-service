import os
import requests
import base64
from typing import List, Dict, Any
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from app.utils.config import Config
from app.utils.logger import get_logger

logger = get_logger(__name__)

def decrypt_password(encrypted_str: str) -> str:
    if not encrypted_str or ':' not in encrypted_str:
        return encrypted_str
        
    try:
        iv_b64, cipher_b64 = encrypted_str.split(':')
        iv = base64.b64decode(iv_b64)
        cipher = base64.b64decode(cipher_b64)
        
        # Parse the 16 bytes key (up to 32 bytes via zero padding)
        key_str = Config.PASSWORD_AES_KEY
        key_padded = key_str.encode('utf-8').ljust(32, b'\0')
        
        aes = AES.new(key_padded, AES.MODE_CBC, iv)
        pt = unpad(aes.decrypt(cipher), AES.block_size).decode('utf-8')
        return pt
    except Exception as e:
        logger.error(f"Failed to decrypt password: {e}")
        return encrypted_str

class BrandFetcher:
    @staticmethod
    def get_all_brands() -> List[Dict[str, Any]]:
        """
        Returns a list of dicts. Each dict contains at least:
        brand_name, host, user, password, database, port
        """
        if Config.TEST_MODE:
            logger.info("TEST_MODE is true. Fetching credentials from .env.local variables.")
            return BrandFetcher._get_brands_from_env()
        else:
            logger.info(f"TEST_MODE is false. Fetching from API: {Config.GET_BRANDS_API}")
            return BrandFetcher._get_brands_from_api()

    @staticmethod
    def _get_brands_from_env() -> List[Dict[str, Any]]:
        brands = []
        if not Config.ACTIVE_BRAND_IDS:
            logger.warning("ACTIVE_BRAND_IDS is empty in env!")
            return brands
            
        active_ids = [x.strip() for x in Config.ACTIVE_BRAND_IDS.split(',') if x.strip()]
        
        for i in range(20):
            brand_id = os.getenv(f"BRAND_ID_{i}")
            if not brand_id or brand_id not in active_ids:
                continue
                
            db_name = os.getenv(f"DB_DATABASE_{i}")
            if not db_name:
                continue
                
            brands.append({
                "brand_name": os.getenv(f"BRAND_NAME_{i}", f"brand_{i}"),
                "host": os.getenv(f"DB_HOST_{i}", "localhost"),
                "user": os.getenv(f"DB_USER_{i}", "root"),
                "password": os.getenv(f"DB_PASSWORD_{i}", ""),
                "database": db_name,
                "port": int(os.getenv(f"DB_PORT_{i}", 3306))
            })
            
        return brands

    @staticmethod
    def _get_brands_from_api() -> List[Dict[str, Any]]:
        headers = {
            "x-pipeline-key": Config.PIPELINE_AUTH_HEADER
        }
        try:
            # 1. Fetch map of {brand_id: brand_name}
            resp = requests.get(Config.GET_BRANDS_API, headers=headers, timeout=10)
            resp.raise_for_status()
            brands_map = resp.json()
            
            brands_data_list = []
            
            # 2. Iterate each brand_id and fetch its schema/credentials
            for brand_id_str, brand_name in brands_map.items():
                try:
                    brand_resp = requests.get(f"{Config.GET_BRANDS_API}/{brand_id_str}", headers=headers, timeout=10)
                    brand_resp.raise_for_status()
                    data = brand_resp.json()
                    
                    decrypted_pw = decrypt_password(data.get("db_password", ""))
                    
                    brands_data_list.append({
                        "brand_name": data.get("brand_name", brand_name),
                        "host": data.get("db_host", "localhost"),
                        "user": data.get("db_user", "root"),
                        "password": decrypted_pw,
                        "database": data.get("db_database", ""),
                        "port": int(data.get("port", 3306))
                    })
                except Exception as subset_e:
                    logger.error(f"Could not fetch credentials for brand_id {brand_id_str}: {subset_e}")
                    
            return brands_data_list
            
        except Exception as e:
            logger.error(f"Failed to fetch initial brands mapping: {e}")
            return []
