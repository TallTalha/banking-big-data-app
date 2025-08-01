# eft_havale_app/data_producer.py
"""
JSON tipteki Transaction objelerini ilgili kafka topiine produce eden ve ana iş akışını yöneten script. 
"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from configs.settings import KAFKA_BOOTSTRAPSERVERS
import logging


LOG = logging.getLogger(__name__)


def create_kafka_producer(kafka_server: str) -> KafkaProducer | None:
    """
    Bu fonksiyon kafka producer nesnesi oluşturur.
        Args:
            None
        Returns:
        producer(KafkaProducer): Kafka producer nesnesi.
    """

    try:
        producer= KafkaProducer(
            bootstrap_servers = kafka_server,
            value_serializer = lambda v: json.dumps(v).encode("utf-8")
        )
        LOG.info("Kafka Producer başarıyla oluşturuldu.")
        return producer
    except KafkaError as ke:
        LOG.critical(f"Kafka Producer nesnesi oluşturulurken hata: {ke}", exc_info=True)
        return None
    