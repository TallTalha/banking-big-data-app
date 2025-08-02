# dwh_app/consumer/data_transformer.py
"""
Bu modül, PostgreSQL'den  consume edilen verileri işlenir hale getirmek için gerekli fonksiyonalrı içerir.
"""

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
import logging

LOG = logging.getLogger(__name__)

def transform_data(df: DataFrame) -> DataFrame:
    """Veri şemasını kontrol eder ve gerekli tip dönüşümlerini yapar."""
    LOG.info("Veri şeması kontrol ediliyor ve dönüşümler uygulanıyor...")
    LOG.info("PostgreSQL'den okunan orijinal şema:")
    df.printSchema()

    # Spark'ın JDBC okuyucusu genellikle tipleri doğru tahmin eder (örn: NUMERIC -> DecimalType).
    # Ancak biz, 'balance' sütununun Double tipinde olmasını garantilemek için
    # açıkça bir tip dönüşümü (casting) yapabiliriz. Bu, veri tutarlılığını sağlar.
    transformed_df = (
        df
        .withColumn("balance", col("balance").cast(DoubleType()))
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )
    
    LOG.info("Dönüştürülmüş nihai şema:")
    transformed_df.printSchema()
    return transformed_df