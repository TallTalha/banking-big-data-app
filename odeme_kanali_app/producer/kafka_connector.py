# odeme_kanali_app/producer/kafka_connector.py
"""
Bu modül, kafka aracına veri produce etmek için Kafka Producer nesnesini oluşturur
ve yapılandırma sorumluluğunu üstlenir.
"""
import logging
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

LOG = logging.getLogger(__name__)

def create_kafka_producer(kafka_server: str) -> KafkaProducer | None :
    """ 
    Açıklama
        Kafka Producer nesnesini oluşturur ve yapılandırır.
    Args:
        kafka_server(str): Kafka aracının bootstrap server adresi.
    Returns:
        producer(KafaProducer|None): Girdilere göre yapılandırılmış Kafka Producer nesnesi, oluşturma sırasında hata olursa None döner.
    """ 
    LOG.info("Kafka Producer oluşturuluyor...")
    try:
        producer = KafkaProducer(
            bootstrap_servers= kafka_server,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
        )
        LOG.info("Kafka Producer nesnesi BAŞARIYLA oluşturuldu.")
        return producer
    except KafkaError as ke:
        LOG.critical(f"Kafka Producer nesnesi oluştururken bağlantı hatası."
                     "Sunucunun çalıştığı ve ssh tünelinin aktif olduğundan emin olun: {ke}", exc_info=True)
        return None
    except Exception as e:
        LOG.critical(f"Kafka Producer nesnesi oluştururken beklenmedik hata: {e}", exc_info=True)
        return None
    
