# eft_havale_app/consumer/consumer.py
"""
Bu modül, kafka topiğinden consume etme işlemini gerçekleştirmek için gerekli fonksiyonalrı içerir, 
spark oturumu oluşturmak ve ilgili kafka topiğinden veri okuma fonksiyonları yer alır.  
"""
from pyspark.sql import SparkSession, DataFrame

import logging

LOG = logging.getLogger(__name__)

def create_spark_session(appName: str, mongo_uri: str) -> SparkSession | None:
    """
    Açıklama:
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
            .config("spark.mongodb.write.connection.uri", mongo_uri)
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
    Açıklama:
        Girdi olarak verilen, Spark Oturumu, Kafka Sercer ve Kafka Topic  kullanılarak topikteki veriler okunur ve DataFrame olarak döndürülür.
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
    
def readStream_from_kafka(spark: SparkSession, kafka_server: str, kafka_topic: str, startingOffsets: str = "latest" ) -> DataFrame | None:
    """
    Açıklama:
        Girdi olarak verilen, Spark Oturumu, Kafka Sercer ve Kafka Topic  kullanılarak topikteki veriler okunur ve DataFrame olarak döndürülür.
    Args:
        spark(SparkSession): Kafka Topiğini okuyacak olan spark oturum nesnesidir.
        kafka_server(String): Consume edilmesi gereken kafka bootstrap server adresi.  
        kafka_topic(String): Consume edilmesi gereken kafka topiğinin adıdır.
        startingOffsets(String): (earliest|latest|<specific_partition>) Topiğin neresinden verilerin okunmaya başlaması gerektiğini belirtir. 
    Returns:
        DataFrame: Okunan verinin spark tarafından işlenebilmesi için DataFrame nesnesine dönüşür.
    """
    try:
        LOG.info(f"{kafka_topic} topiğinden veriler okunuyor...")
        df = (
            spark.readStream
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
    
def write_batch_to_mongo(df: DataFrame, collection: str) -> None:
    """
    Açıklama:
        Bir Spark Batch DataFrame'ini belirtilen MongoDB koleksiyonuna yazar.
        Varolan verinin üzerine yazar (overwrite)
    Args:
        df(DataFrame): Mongo'ya yazılacak veri yapısı.
        collection(str): Verinin yazılacağı koleksiyon ismi.
    Returns:
        None
    """
    LOG.info(f"{collection} koleksiyonuna BATCH veri yazılıyor...")
    try:
        (
            df.write
            .format("mongodb")
            .mode("overwrite")
            .option("collection",collection)
            .save()
        )
        LOG.info(f"{collection} koleksiyona başarıyla BATCH veriler yazıldı.")
    except Exception as e:
        LOG.critical(f"{collection} koleksiyonuna BATCH veri yazılırken hata: {e}", exc_info=True)

def write_stream_to_mongo(df: DataFrame, collection: str, checkpoint_location: str):
    """
    Açıklama:
        Bir Spark Streaming DataFrame'ini belirtilen MongoDB koleksiyonuna yazar.
        Her batch'te tüm tabloyu günceller ve üstüne yazar (complete & overwrite mode).
    Args:
        df(DataFrame): Mongo'ya yazılacak veri yapısı.
        collection(str): Verinin yazılacağı koleksiyon ismi.
    Returns:
        None
    """
    LOG.info(f"{collection} koleksiyonuna STREAM veri yazılıyor...")
    return (
        df.writeStream
        .outputMode("complete")
        .foreachBatch(
            lambda batch_df, epoch_id: batch_df.write
            .format("mongodb")
            .mode("overwrite")
            .option("collection", collection)
            .option("operationType", "replace")
            .option("replaceDocument","_id") 
            .save()
        )
        .option("checkpointLocation", checkpoint_location) 
        .trigger(processingTime='15 seconds') 
        .start()
    )



