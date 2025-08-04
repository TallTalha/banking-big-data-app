# odeme_kanali_app/consumer/data_transformer.py
"""
Bu modül, ham Spark DataFrame'lerini alan ve onları analiz için
temiz, yapısal ve zenginleştirilmiş bir formata dönüştüren (transform)
tüm iş mantığını içerir.
"""

import logging
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType,TimestampType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

LOG = logging.getLogger(__name__)

def transfrom_transaction(raw_df: DataFrame) -> DataFrame:
    """
    Açıklama:
        Ham Kafka verisini (JSON string) alır, onu yapısal bir DataFrame'e dönüştürür
        ve gerekli tip dönüşümlerini uygular.
    Args:
        raw_df (DataFrame): 'value' sütununda ham JSON verisi içeren DataFrame.
    Returns:
        final_df(DataFrame): Parse edilmiş, temizlenmiş ve tipleri düzeltilmiş DataFrame.
    """
    transaction_schema = StructType([
        StructField("pid", StringType()),
        StructField("timestamp", StringType()),
        StructField("oid", StringType()),
        StructField("name", StringType()),
        StructField("balance", StringType()),
        StructField("btype", StringType()),
        StructField("ptype", IntegerType())
    ])

    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema=transaction_schema).alias("data")
    ).select("data.*")

    final_df = (
        parsed_df
        .withColumn("amount", F.col("balance").cast(DoubleType()))
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
    )
    LOG.info("Veri dönüştürme tamamlandı. Nihai şema:")
    final_df.printSchema()

    return final_df