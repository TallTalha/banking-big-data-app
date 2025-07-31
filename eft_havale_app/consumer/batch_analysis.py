from data_transformer import transform_transactions
from data_consumer import create_spark_session, read_from_kafka
from configs.settings import KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC
from pyspark.sql import functions as F

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__)) # .../consumer
app_root_dir = os.path.dirname(current_dir)             # .../eft_havale_app
project_root = os.path.dirname(app_root_dir)            # .../banking-big-data-app
sys.path.append(project_root)

import logging
from utils.logger import set_logger

set_logger("consumer_batch_analysis",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

def main():
    """
    Açıkklama:
        Batch verilerin işlenmesinde iş akışını düzenler.
    """ 
    spark = create_spark_session(appName="eftHavaleBatchAnalysis")
    if not spark:
        LOG.critical("Spark Session=None, işlem sonlandırıldı. ")
        sys.exit(1) # ÇIKIŞ -> Spark oturumu oluşmadı.

    raw_df = read_from_kafka(spark=spark, kafka_server=KAFKA_BOOTSTRAPSERVERS, kafka_topic=KAFKA_TOPIC) #type: ignore
    if not raw_df:
        LOG.info("Kafka topiğinde işlenecek veri bulunamadı. İşlem sonlandırıldı.")
        sys.exit(1) # ÇIKIŞ -> İşlenecek veri yok.

    transaction_df = transform_transactions(raw_df=raw_df)
    transaction_df.cache()

    #Lazy Evulation Dolayısyla Hata Kontrolü spark-action işleminden önce başlatıldı.
    try:
        LOG.info(f"{transaction_df.count()} adet işlem kaydı anliz edilecek...")
        
        # Analiz 1: 
        LOG.info("--- Analiz 1: Bankalara  Aktarılan Toplam Para Hacmi ---")
        bank_based_money_volume = (
            transaction_df
            .groupBy(F.col("info").getItem(2))
            .agg(F.sum("amount").alias("toplam_hacim"))
        )
        bank_based_money_volume.show()

        # Analiz 2: 
        LOG.info("--- Analiz 2: Günlük İşlem Sayısı ve Hacim ---")
        daily_volume_and_count =(
            transaction_df
            .agg(
                F.sum("amount").alias("toplam_hacim"),
                F.sum("pid").alias("toplam_islem_sayisi")    
            )
        )   
        daily_volume_and_count.show()
    except Exception as e:
        LOG.critical(f"Batch analizi sırasında hata:{e}", exc_info=True)
    finally:
        spark.stop()
        LOG.info("Spark Session sonlandırıldı.")

if __name__ == "__main__":
    main()