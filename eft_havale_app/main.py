from eft_havale_data_gen import generate_transactions, create_user_pool
from eft_havale_producer import create_kafka_producer

import sys
from time import sleep


from configs.settings import KAFKA_TOPIC, TRANSACTION_COUNT, USER_COUNT
from utils.logger import setup_logger


LOG = setup_logger("main_eft_havale_app")

def main():
    """
    Ana iş akışını düzenler.
    """
    producer = create_kafka_producer()

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
            
            sleep(0.005) # 5ms bekle
    except Exception as e:
        LOG.error(f"Kafka Topiğine veri gönderirken hata:{e}", exc_info=True)

    finally:
        LOG.info("(flush) Tüm mesajların gönderilmesi bekleniyor.")
        producer.flush()
        producer.close()
        LOG.info("Kafka Producer kapatıldı. Producer Scripti tamamlandı.")

if __name__ == "__main__":
    main()