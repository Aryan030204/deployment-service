import os

from dotenv import load_dotenv


load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "brands-topic")
