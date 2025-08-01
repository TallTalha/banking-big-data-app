from eft_havale_app.producer.data_generator import generate_transactions, create_user_pool
from eft_havale_app.producer.data_producer import create_kafka_producer

import sys
from time import sleep
import os

from configs.settings import KAFKA_TOPIC, KAFKA_BOOTSTRAPSERVERS, TRANSACTION_COUNT, USER_COUNT


current_dir = os.path.dirname(os.path.abspath(__file__)) # .../producer
app_root_dir = os.path.dirname(current_dir)             # .../eft_havale_app
project_root = os.path.dirname(app_root_dir)            # .../banking-big-data-app
sys.path.append(project_root)

import logging
from utils.logger import set_logger

set_logger("producer_main",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

def main():
    """
    Ana iş akışını düzenler.
    """
    producer = create_kafka_producer(KAFKA_BOOTSTRAPSERVERS) #type: ignore

    if not producer:
        print("Producer is None->sys.exit(1)")
        sys.exit(1) # ÇIKIŞ -> Kafka Nesnesi oluşmadı.

    user_pool = create_user_pool(USER_COUNT)

    LOG.info(f"{KAFKA_TOPIC} topiğine, {TRANSACTION_COUNT} adet transaction gönderiliyor.")
    
    try:
        for i in range(TRANSACTION_COUNT):
            transaction = generate_transactions(user_pool)
            producer.send(KAFKA_TOPIC, value=transaction)

            if (i+1)%500 == 0:
                LOG.info(f"{i+1} adet veri gönderildi.")
            
            sleep(0.5) # 50ms bekle
    except Exception as e:
        LOG.error(f"Kafka Topiğine veri gönderirken hata:{e}", exc_info=True)

    finally:
        LOG.info("(flush) Tüm mesajların gönderilmesi bekleniyor.")
        producer.flush()
        producer.close()
        LOG.info("Kafka Producer kapatıldı. Producer Scripti tamamlandı.")

if __name__ == "__main__":
    main()