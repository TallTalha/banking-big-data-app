# eft_havale_app/consumer/data_transformer.py
"""
Bu modül, kafka topiğinden consume edilen verileri işlenir hale getirmek için gerekli fonksiyonalrı içerir.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StringType, DoubleType, StructType

import logging

LOG = logging.getLogger(__name__)

def transform_transactions(raw_df: DataFrame) -> DataFrame:
    """
    Açıklama: Raw olarak gelen transaction verilerini, uygun şema giydirilerek,
        spark tarafından işlenebilir veri yapısına dönüştürülür.
    Args:
        raw_df(DataFrame): Dönüştürülecek olan raw DataFrame.
    Returns:
        final_df(Dataframe):Spark tarafından işlenebilir veri yapısı.
    """
    LOG.info("Ham veriler dönüştürülüyor...")
    
    schema = StructType([
        StructField("pid",StringType()),
        StructField("timestamp",StringType()),
        StructField("ptype",StringType()),
        StructField("account",StructType([
            StructField("oid",StringType()),
            StructField("name",StringType()),
            StructField("iban",StringType())
        ])),
        StructField("info",StructType([
            StructField("name",StringType()),
            StructField("iban",StringType()),
            StructField("bank",StringType()),
        ])),
        StructField("balance",StringType()),
        StructField("btype",StringType())
        
    ])

    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), schema=schema).alias("transaction")
    ).select("transaction.*")

    final_df = (
        parsed_df
        .withColumn("amount", F.col("balance").cast(DoubleType()))
        .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )
    LOG.info("Ham veriler dönüştürüldü.")
    return final_df
