# eft_havale_app/consumer/consumer.py
"""
Bu modül, kafka topiğinden consume etme işlemini gerçekleştirmek için gerekli fonksiyonalrı içerir, 
spark oturumu oluşturmak ve ilgili kafka topiğinden veri okuma fonksiyonları yer alır.  
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StringType, DoubleType

import logging

LOG = logging.getLogger("data_producer")

def create_spark_session(appName: str) -> SparkSession | None:
    """
    Spark session oluşturur ve döndürür.
        Args:
            appName(String): Spark Oturumunun ismi.
        Returns:
            SparkSession: Fonksiyon ile oluşturulan Spark oturum nenesidir.
    """
    try:
        LOG.info("Spark Session Oluşturuluyor...")
        spark = (
            SparkSession.builder
            .appName(appName)
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("Spark başarıyla oluşturuldu.")
        return spark
    except Exception as e:
        LOG.critical(f"Spark Session oluşturulurken hata: {e}",exc_info=True)
        return None
    
def read_from_kafka(spark: SparkSession, kafka_server: str, kafka_topic: str, startingOffsets: str = "earliest" ) -> DataFrame | None:
    """
    Girdi olarak verilen, Spark Oturumu, Kafka Sercer ve Kafka Topic  kullanılarak
    topikteki veriler okunur ve DataFrame olarak döndürülür.
        Args:
            spark(SparkSession): Kafka Topiğini okuyacak olan spark oturum nesnesidir.
            kafka_server(String): Consume edilmesi gereken kafka bootstrap server adresi.  
            kafka_topic(String): Consume edilmesi gereken kafka topiğinin adıdır.
            startingOffsets(str): (earliest|latest|<specific_partition>) Topiğin neresinden verilerin okunmaya başlaması gerektiğini belirtir. 
        Returns:
            DataFrame: Okunan verinin spark tarafından işlenebilmesi için DataFrame nesnesine dönüşür.
    """
    try:
        LOG.info(f"{kafka_topic} topiğinden veriler okunuyor...")
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_server)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", startingOffsets)
            .load()
        )
        LOG.info(f"{kafka_topic} topiğinden veriler başarıyla okundu.")
        return df
    except Exception as e:
        LOG.error(f"{kafka_topic} topiğinden veriler okunurken hata: {e}", exc_info=True)
        return None
    