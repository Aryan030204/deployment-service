import os
import mysql.connector
from pymongo import MongoClient
from kafka_client.producer import publish_brand_event
from shared.utils.logger import get_logger

logger = get_logger("deployment-service.delete-tenant")

def delete_tenant_logic(brand_id: int):
    logger.info(f"🚀 Starting deletion logic for brand ID: {brand_id}")
    
    # DB credentials from environment (should be loaded by caller)
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    MASTER_DB = os.getenv("MASTER_DB")
    SPEED_DB_URI = os.getenv("SPEED_DB_URI")

    # Connect to MySQL
    try:
        mysql_conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            database=MASTER_DB
        )
        cursor = mysql_conn.cursor(dictionary=True)
    except Exception as e:
        logger.error(f"❌ Failed to connect to MySQL: {e}")
        return False, f"MySQL Connection Error: {e}"

    try:
        # 1. Fetch brand details
        cursor.execute("SELECT name, db_name FROM brands WHERE id = %s", (brand_id,))
        brand = cursor.fetchone()
        if not brand:
            logger.warn(f"❌ Brand ID {brand_id} not found in master.brands")
            return False, f"Brand ID {brand_id} not found."

        brand_name = brand['name']
        db_name = brand['db_name']
        logger.info(f"🔍 Found brand: {brand_name}, DB: {db_name}")

        # Start Kafka Notification
        publish_brand_event({"brand_id": brand_id, "brand_name": brand_name, "state": "deleting_started"})

        # 2. Drop Database and Delete from master.brands
        logger.info(f"🗑️ Dropping database {db_name}...")
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        except Exception as e:
            logger.error(f"⚠️ Error dropping database: {e}")
        
        logger.info("🗑️ Deleting entry from master.brands...")
        cursor.execute("DELETE FROM brands WHERE id = %s", (brand_id,))
        mysql_conn.commit()

        publish_brand_event({"brand_id": brand_id, "brand_name": brand_name, "state": "mysql_deletion_completed"})

        # 3. MongoDB Deletion
        logger.info("🗑️ Connecting to MongoDB clusters...")
        ARCH_DB_URI = os.getenv("ARCH_DB_URI")
        SPEED_DB_URI = os.getenv("SPEED_DB_URI")

        try:
            # --- cluster 1: ARCH_DB (Tenants & PipelineCreds) ---
            if ARCH_DB_URI:
                logger.info("📡 Connecting to ARCH_DB...")
                arch_client = MongoClient(ARCH_DB_URI)
                # The URI might already have the DB name, but we fallback to "arch-auth"
                arch_db = arch_client.get_default_database() if "/arch-auth" in ARCH_DB_URI else arch_client["arch-auth"]
                
                logger.info(f"🗑️ Deleting from {arch_db.name}.tenants and {arch_db.name}.pipelinecreds...")
                t_res = arch_db["tenants"].delete_many({
                    "$or": [{"database": db_name}, {"brand_num": brand_id}, {"brand_num": str(brand_id)}, {"brand_id": brand_name}]
                })
                c_res_arch = arch_db["pipelinecreds"].delete_many({
                    "$or": [{"brand_name": brand_name}, {"brand_id": brand_id}, {"brand_id": str(brand_id)}, {"db_database": db_name}]
                })
                logger.info(f"✅ ARCH_DB: Deleted {t_res.deleted_count} tenants, {c_res_arch.deleted_count} pipelinecreds.")
            else:
                logger.warning("⚠️ ARCH_DB_URI not found in environment")

            # --- cluster 2: SPEED_DB (PageSpeed & PipelineCreds) ---
            if SPEED_DB_URI:
                logger.info("📡 Connecting to SPEED_DB...")
                speed_client = MongoClient(SPEED_DB_URI)
                
                # Delete items in pagespeed_brands (Analytics cluster)
                ps_db = speed_client["pagespeed_brands"]
                p_res = ps_db["brands"].delete_many({"brand_name": brand_name})
                
                # User mentioned pipelinecreds also exists here
                # We check "arch-auth" database on this cluster as well
                speed_auth_db = speed_client["arch-auth"]
                c_res_speed = speed_auth_db["pipelinecreds"].delete_many({
                    "$or": [{"brand_name": brand_name}, {"brand_id": brand_id}, {"brand_id": str(brand_id)}, {"db_database": db_name}]
                })
                
                logger.info(f"✅ SPEED_DB: Deleted {p_res.deleted_count} pagespeed records, {c_res_speed.deleted_count} pipelinecreds.")
            else:
                logger.warning("⚠️ SPEED_DB_URI not found in environment")

            publish_brand_event({"brand_id": brand_id, "brand_name": brand_name, "state": "mongo_deletion_completed"})
        except Exception as e:
            logger.error(f"⚠️ Error during MongoDB deletion: {e}")
            # We continue even if Mongo fails so MySQL state is consistent

        # 4. Final Success Notification
        logger.info(f"✅ Deletion successful for brand ID: {brand_id}")
        publish_brand_event({"brand_id": brand_id, "brand_name": brand_name, "state": "deleting_success"})
        return True, "Success"

    except Exception as e:
        logger.error(f"❌ Error during deletion: {e}")
        return False, str(e)
    finally:
        cursor.close()
        mysql_conn.close()
