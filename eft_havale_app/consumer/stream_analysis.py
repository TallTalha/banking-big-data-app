from data_transformer import transform_transactions
from data_consumer import create_spark_session, readStream_from_kafka
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

set_logger("consumer_stream_analysis",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

def main():
    """
    Açıkklama:
        Real-time verilerin işlenmesinde iş akışını düzenler.
    """ 
    spark = create_spark_session(appName="eftHavaleBatchAnalysis")
    if not spark:
        LOG.critical("Spark Session=None, işlem sonlandırıldı. ")
        sys.exit(1) # ÇIKIŞ -> Spark oturumu oluşmadı.

    raw_stream_df = readStream_from_kafka(
        spark=spark, kafka_server=KAFKA_BOOTSTRAPSERVERS, kafka_topic=KAFKA_TOPIC, startingOffsets="latest" )  #type: ignore

    transaction_df = transform_transactions(raw_df=raw_stream_df) #type: ignore

    # Banka şirketine göre sürekli güncellenen toplam hacim
    bank_based_volume_strream = (
        transaction_df
        .groupBy(F.col("info.bank"))
        .agg(
            F.sum("amount").alias("toplam_hacim"),
            F.count("pid").alias("toplam_islem_sayisi")    
        )
    )

    query = (
        bank_based_volume_strream
        .writeStream
        .outputMode("complete")
        .format("console")
        .option("checkpointLocation", "checkpoint/finance_stream_v1") 
        .trigger(processingTime='10 seconds') 
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()