import json
from kafka import KafkaProducer
from app.utils.config import Config
from app.utils.logger import get_logger
from app.models.kafka_event import UtmAggregateCompletedEvent, UtmAggregateFailedEvent

logger = get_logger(__name__)

class ProducerService:
    def __init__(self):
        self.topic = Config.KAFKA_TOPIC
        self.producer = KafkaProducer(
            bootstrap_servers=[Config.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def publish_completed(self, event_data: UtmAggregateCompletedEvent):
        try:
            future = self.producer.send(self.topic, event_data.model_dump())
            result = future.get(timeout=10)
            logger.info(f"Published completion event for {event_data.jobId} to {self.topic}")
        except Exception as e:
            logger.error(f"Failed to publish completion event: {e}")

    def publish_failed(self, event_data: UtmAggregateFailedEvent):
        try:
            future = self.producer.send(self.topic, event_data.model_dump())
            result = future.get(timeout=10)
            logger.info(f"Published failure event for {event_data.jobId} to {self.topic}")
        except Exception as e:
            logger.error(f"Failed to publish failure event: {e}")
            
    def close(self):
        self.producer.close()
