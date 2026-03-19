# -*- coding: utf-8 -*-
"""
Standalone Database Clone Script
Replicates Target DB from Source DB with exact schemas tables dynamically based on master.brands and Kafka triggers.
"""

import os
import sys
import json
import mysql.connector
from confluent_kafka import Consumer

# Add config path for settings
service_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

from config.settings import KAFKA_BROKER, KAFKA_TOPIC

# Load .env relative from root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(root_dir, ".env"))

# DB Connection for creation setup (requires Create/Select permissions)
DB_HOST = os.getenv("MYSQL_ROOT_HOST", os.getenv("DB_HOST", "localhost"))
DB_USER = os.getenv("MYSQL_ROOT_USER", os.getenv("DB_USER", "root"))
DB_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD", os.getenv("DB_PASSWORD", ""))
DB_PORT = int(os.getenv("DB_PORT", "3306"))
MASTER_DB = os.getenv("MASTER_DB", "master")

def get_source_db_name():
    """
    Fetches the latest added brand's db_name from master.brands.
    """
    try:
        cnx = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            database=MASTER_DB,
            ssl_disabled=False
        )
        cursor = cnx.cursor()
        query = "SELECT db_name FROM brands ORDER BY id DESC LIMIT 1;"
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        cnx.close()
        if result:
            return result[0]
        return None
    except Exception as e:
        print(f"❌ Error fetching source DB name: {e}")
        return None

def clone_database(source_db, target_db):
    """
    Clones the source database structure into target database.
    """
    if not source_db or not target_db:
         print("❌ Missing Source or Target Database name.")
         return

    print(f"🔄 Starting cloning workflow: {source_db} ➡️ {target_db}")

    try:
        cnx = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            ssl_disabled=False
        )
        cursor = cnx.cursor()

        # 1. Create target database if not exists
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS `{target_db}`;"
        print(f"🛠️ Executing: {create_db_sql}")
        cursor.execute(create_db_sql)

        # 2. Fetch all tables from source database
        fetch_tables_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s;"
        cursor.execute(fetch_tables_sql, (source_db,))
        tables = cursor.fetchall()

        if not tables:
             print(f"⚠️ No tables found in source database: {source_db}")
             cursor.close()
             cnx.close()
             return

        print(f"📦 Found {len(tables)} tables to clone.")

        # 3. Create tables LIKE source tables
        for (table_name,) in tables:
            clone_sql = f"CREATE TABLE IF NOT EXISTS `{target_db}`.`{table_name}` LIKE `{source_db}`.`{table_name}`;"
            print(f"🛠️ Executing: {clone_sql}")
            try:
                cursor.execute(clone_sql)
            except Exception as e:
                print(f"❌ Error cloning table {table_name}: {e}")

        cnx.commit()
        cursor.close()
        cnx.close()
        print(f"✅ Cloning complete: {source_db} ➡️ {target_db}")

    except Exception as e:
        print(f"❌ Critical error during cloning: {e}")


def process_event(data: dict) -> bool:
    target_db = (
        data.get("target_db_name")
        or data.get("brand_tag")
        or ((data.get("tenant_payload") or {}).get("database"))
        or data.get("brand_name")
    )
    if not target_db:
        print("⚠️ Missing target_db_name/brand_tag/tenant_payload.database/brand_name in payload.")
        return False

    source_db = get_source_db_name()
    if not source_db:
        print("❌ Failed to resolve Source Database from master.brands.")
        return False

    if source_db.lower() == str(target_db).lower():
        print("❌ Source and Target databases are the same. Skipping clone.")
        return False

    clone_database(source_db, target_db)
    return True

def run():
    print("🚀 Starting Database Clone Consumer")
    
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "clone-database-service-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    print(f"📡 Subscribed to Kafka Topic: {KAFKA_TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            try:
                payload = message.value().decode("utf-8")
                print(f"📩 Received Event: {payload}")
                
                data = json.loads(payload)
                process_event(data)

            except Exception as e:
                print(f"❌ Error processing message: {e}")

    finally:
        consumer.close()

if __name__ == "__main__":
    run()
