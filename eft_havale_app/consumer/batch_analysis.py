# eft_havale_app/consumer/batch_analysis.py
"""
Bu script, batch analizleri için iş akışını yönetir ve analiz sonuçlarını MongoDB'ye yazar. Batch işlemler: 
    -   Bankalara  Aktarılan Toplam Para Hacmi
    -   Belirli Bir Ayın Haftalık İşlem Sayıları ve Hacimleri
"""
from data_transformer import transform_transactions, get_last_week
from data_consumer import create_spark_session, read_from_kafka, write_batch_to_mongo
from configs.settings import KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC, MONGO_URI
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
    spark = create_spark_session(appName="eftHavaleBatchAnalysis",mongo_uri=MONGO_URI) 
    if not spark:
        LOG.critical("Spark Session=None, işlem sonlandırıldı. ")
        sys.exit(1) # ÇIKIŞ -> Spark oturumu oluşmadı.

    raw_df = read_from_kafka(spark=spark, kafka_server=KAFKA_BOOTSTRAPSERVERS, kafka_topic=KAFKA_TOPIC) 
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
            .groupBy(F.col("info.bank"))
            .agg(F.sum("amount").alias("toplam_hacim"))
        ).orderBy(F.desc("toplam_hacim"))
        bank_based_money_volume.show()
        write_batch_to_mongo(bank_based_money_volume,collection="bank_based_stats_2025")

        # Analiz 2: 
        LOG.info("--- Analiz 2: Bir Önceki Haftanın İşlem Sayıları ve Hacimleri ---")
        
        start_of_last_week, end_of_last_week =  get_last_week()

        last_week_df = (
            transaction_df
            .withColumn("ts_date", F.to_date("timestamp"))
            .filter(
                (F.col("ts_date") >= F.lit(str(start_of_last_week))) &
                (F.col("ts_date") <= F.lit(str(end_of_last_week)))
            )
        )
        last_week_stats = (
            last_week_df
            .agg(
                F.sum("amount").alias("gecen_hafta_toplam_hacim"),
                F.count("pid").alias("gecen_hafta_islem_sayisi")
            )
        )
        last_week_stats.show()
        write_batch_to_mongo(last_week_stats, collection="last_week_stats")
        
    except Exception as e:
        LOG.critical(f"Batch analizi sırasında hata:{e}", exc_info=True)
    finally:
        spark.stop()
        LOG.info("Spark Session sonlandırıldı.")

if __name__ == "__main__":
    main()