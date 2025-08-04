# odeme_kanali_app/consumer/run_batch_analysis.py
"""
Kafka üzerinden ödeme kanalına ait verileri okuyup Spark ile dönüştüren ve batch analiz iş akışını yürüten modüldür.
    -   Spark oturumu başlatılır
    -   Kafka'dan veri okunur
    -   Veriler dönüştürülür ve kontrol edilir
    -   Analizler gerçekleştirilir
    -   Analiz Sonuçları MongoDB'ye yazdırılır
Log kayıtları 'logs/run_batch_analysis.log' dosyasına yazılır.
"""

import sys
import os
import logging

current_dir = os.path.dirname(os.path.abspath(__file__)) # /consumer
app_dir = os.path.dirname(current_dir) # /odeme_kanali_app
project_dir = os.path.dirname(app_dir) # /banking-big-data-app
sys.path.append(project_dir)

from utils.logger import set_logger
from configs.settings import KAFKA_BOOTSTRAPSERVERS, ODEME_KANALI_KAFKA_TOPIC
from odeme_kanali_app.consumer.data_consumer import create_spark_session, read_from_kafka, write_to_mongo
from odeme_kanali_app.consumer.data_transformer import transfrom_transaction
from odeme_kanali_app.consumer.analysis_logic import pivot_payment_types,find_qr_code_candidates,find_visual_card_candidates 
import pyspark.sql.functions as F

set_logger(name="run_batch_analysis", app_file_path=app_dir)

def main():
    """
    Ana iş akışını yönetir.    
    """
    LOG = logging.getLogger(__name__)

    LOG.info("Batch Analizi başlatıldı.")

    spark = create_spark_session(appName="OdemeKanaliBatchAnalysis")
    if not spark:
        sys.exit(1) # Çıkış: Spark Session oluşturulamadı.

    try:

        raw_df = read_from_kafka(spark=spark, kafka_server=KAFKA_BOOTSTRAPSERVERS, kafka_topic=ODEME_KANALI_KAFKA_TOPIC)
        if raw_df is None or raw_df.isEmpty():
            LOG.warning("İşlenecek veri bulunamadı.")
            sys.exit(1) # İşlenecek veri bulunamadı.

        transaction_df = transfrom_transaction(raw_df=raw_df)
        transaction_df.cache()

        # Action (Test için)
        LOG.info("Dönüştürülmüş verinin ilk 10 satırı gösteriliyor:")
        transaction_df.show(10, truncate=False)
        
        LOG.info(f"{transaction_df.count()} adet Transaction analiz edilecek...")
        #Analiz-1:
        LOG.info("Analiz-1: Kullanıcı bazında, her bir ödeme tipinin kaç kez kullanıldığını gösteren pivot tablo oluşturuluyor...")
        pivot_df = pivot_payment_types(transaction_df)
        pivot_df.show(10, truncate=False)
        write_to_mongo(df=pivot_df, collection="odeme_tipi_pivot")


        #Analiz-2
        LOG.info("Analiz-2: QRCode kampanyası için hedef kitle analizi yapılıyor...")
        chip_usage_avg =  pivot_df.agg(F.avg("1000")).collect()[0][0]
        qr_candidates = find_qr_code_candidates(pivot_df, chip_threshold=chip_usage_avg)
        qr_candidates.show(10, truncate=False)
        write_to_mongo(df=qr_candidates, collection="qr_adaylari")

        #Analiz-3:
        LOG.info("Analiz-2: Sanal Kart kampanyası için hedef kitle analizi yapılıyor...")
        mail_order_usage_avg = pivot_df.agg(F.avg("2000")).collect()[0][0]
        visual_card_candidates = find_visual_card_candidates(pivot_df=pivot_df, mail_order_threshold=mail_order_usage_avg)
        visual_card_candidates.show(10, truncate=False)
        write_to_mongo(df=visual_card_candidates, collection="sanal_kart_adaylari")

    except Exception as e:
        LOG.critical(f"Ana iş akışında hata oluştu: {e}", exc_info=True)
    finally:
        if spark:
            spark.stop()
            LOG.info("Spark oturumu sonlandırıldı.")

if __name__ == "__main__":
    main()

