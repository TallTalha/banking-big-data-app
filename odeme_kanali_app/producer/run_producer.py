# odeme_kanali_app/producer/run_producer.py
"""
Bu script, Ödeme Kanalı Analizi projesi için veri üreticisinin (producer)
ana giriş noktasıdır (entry point).

İş Akışı:
    1.   Gerekli konfigürasyonları ve logger'ı ayarlar.
    2.   Bir Kafka producer ve sahte kullanıcı havuzu oluşturur.
    3.   Belirlenen sayıda sahte finansal işlem verisi üretir.
    4.   Üretilen her bir veriyi Kafka'daki ilgili topic'e gönderir.
    5.   İşlem bittiğinde kaynakları temiz bir şekilde kapatır.
"""
import logging
import os
import sys
from time import sleep

# Bu script'in, bir üst dizindeki 'utils' ve 'configs' klasörlerini görebilmesi için
# projenin ana kök dizinini Python'un arama yoluna ekliyoruz.
current_dir = os.path.dirname(os.path.abspath(__file__)) # /producer
app_dir = os.path.dirname(current_dir) # /odeme_kanali_app
project_dir = os.path.dirname(app_dir) # /banking-big-data-app
sys.path.append(project_dir)

from utils.logger import set_logger
from configs.settings import KAFKA_BOOTSTRAPSERVERS, ODEME_KANALI_KAFKA_TOPIC, ODEME_KANALI_TRANSACTION_COUNT, ODEME_KANALI_USER_COUNT

set_logger(name="run_producer",app_file_path=app_dir)

from .simulator import create_user_pool, generate_payment_transaction
from .kafka_connector import create_kafka_producer


def main():
    """Ana iş akışını yönetir."""
    LOG = logging.getLogger(__name__)
    LOG.info("odeme_kanali_app/run_producer uygulaması başlatılıyor...")

    producer = create_kafka_producer(kafka_server=KAFKA_BOOTSTRAPSERVERS)
    if not producer:
        LOG.critical("Kafka Producer nesnesi oluşmadı. Uygulama sonlandırıldı.")
        sys.exit(1) # ÇIKIŞ -> Kafka Producer nesnesi oluşmadı.

    user_pool = create_user_pool(ODEME_KANALI_USER_COUNT)
    sent_count = 0  # gönderilen kayıt sayısı
    try:
        
        LOG.info(f"{ODEME_KANALI_KAFKA_TOPIC} topiğine {ODEME_KANALI_TRANSACTION_COUNT} adet transaction gönderilecek.")
        
        for i in range(ODEME_KANALI_TRANSACTION_COUNT):
            
            transaction = generate_payment_transaction(user_pool=user_pool)
            producer.send(topic=ODEME_KANALI_KAFKA_TOPIC, value=transaction)
            sent_count+=1
            if ((i+1)%1000==0):
                LOG.info(f"{(i+1)} adet transaction {ODEME_KANALI_KAFKA_TOPIC} topiğine gönderildi.")

            sleep(0.05) #5ms bekle

    except Exception as e:
        LOG.error(f"Veri üretim ve gönderim döngüsünde beklenmedik hata:{e}", exc_info=True)
    finally:
        oran = (sent_count/ODEME_KANALI_TRANSACTION_COUNT)*100
        LOG.info(f"{ODEME_KANALI_KAFKA_TOPIC} topiğine veri gönderimi döngüsü tamamlandı. Kafka producer kapatılıyor...")
        LOG.info(f"{sent_count}/{ODEME_KANALI_TRANSACTION_COUNT} --- %{oran} tamamlandı. ")
        if producer:
            producer.flush() # Tamponda bekleyen tüm mesajların gönderildiğinden emin ol.
            producer.close() # Bağlantıyı temiz bir şekilde kapat.
        LOG.info("odeme_kanali_app/run_producer uygulaması sonlandırıldı.")

if __name__=="__main__":
    main()

    