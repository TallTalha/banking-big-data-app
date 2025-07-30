# producer/eft_havale_producer.py
"""
JSON tipteki Transaction objelerini ilgili kafka topiine produce eden ve ana iş akışını yöneten script. 
"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from configs.settings import KAFKA_BOOTSTRAPSERVERS
from utils.logger import setup_logger


LOG = setup_logger("eftHavaleProducer")


def create_kafka_producer() -> KafkaProducer | None:
    """
    Bu fonksiyon kafka producer nesnesi oluşturur.
        Args:
            None
        Returns:
        producer(KafkaProducer): Kafka producer nesnesi.
    """

    try:
        producer= KafkaProducer(
            bootstrap_servers = KAFKA_BOOTSTRAPSERVERS,
            value_serializer = lambda v: json.dumps(v).encode("utf-8")
        )
        LOG.info("Kafka Producer başarıyla oluşturuldu.")
        return producer
    except KafkaError as ke:
        LOG.critical(f"Kafka Producer nesnesi oluşturulurken hata: {ke}", exc_info=True)
        return None
    