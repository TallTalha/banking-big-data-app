# odeme_kanali_app/consumer/data_consumer.py
"""
Bu modül, Spark Session oluşturma ve veri kaynaklarından (örn: Kafka)
veri okuma gibi veri tüketme (consumption) işlemlerini içerir.
Bu modül, projenin "Extract" (Veri Çekme) katmanını temsil eder.
"""
import logging
from pyspark.sql import SparkSession, DataFrame

LOG = logging.getLogger(__name__)

def create_spark_session(appName: str) -> SparkSession | None:
    """
    Açıklama:
        Yapılandırılmış bir Spark Session oluşturur ve döndürür.
    Args:
        app_name (str): Spark uygulamasının adı.
    Returns:
        spark(SparkSession | None): Başarılı olursa SparkSession nesnesi, olmazsa None döner.
    """
    LOG.info(f"{appName} - Spark Session oluşturuluyor...")

    try:
        spark = (
            SparkSession.builder
            .appName(appName)
            .master("local[*]")
            .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/odeme_kanali_db")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        LOG.info(f"{appName} Spark Session başarıyla oluşturuldu.")
        return spark
    except Exception as e:
        LOG.critical(f"{appName} Spark Session oluştururken hata: {e}", exc_info=True)
        return None
    
def read_from_kafka(spark: SparkSession, kafka_server: str, kafka_topic: str, startingOffsets: str = "earliest" ) -> DataFrame | None:
    """
    Açıklama:
        Girdi olarak verilen, Spark Oturumu, Kafka Server ve Kafka Topic  kullanılarak topikteki veriler okunur ve DataFrame olarak döndürülür.
    Args:
        spark(SparkSession): Kafka Topiğini okuyacak olan spark oturum nesnesidir.
        kafka_server(String): Consume edilmesi gereken kafka bootstrap server adresi.  
        kafka_topic(String): Consume edilmesi gereken kafka topiğinin adıdır.
        startingOffsets(str): (earliest|latest|<specific_partition>) Topiğin neresinden verilerin okunmaya başlaması gerektiğini belirtir. 
    Returns:
        df(DataFrame | None): Okunan verinin spark tarafından işlenebilmesi için Spark DataFrame nesnesine dönüşür, eğer hata oluşursa None döner.
    """
    LOG.info(f"{kafka_topic} topiğinden veriler okunuyor...")
    try:
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
        LOG.critical(f"{kafka_topic} topiğinden veri okurken hata: {e}", exc_info=True)
        return None
    
def write_to_mongo(df: DataFrame, collection: str) -> None:
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
            df.write.format("mongodb") 
            .mode("overwrite") 
            .option("collection", collection) 
            .save()
        )
            
        LOG.info("Veri MongoDB'ye başarıyla yazıldı.")
    except Exception as e:
        LOG.critical(f"{collection} koleksiyonuna BATCH veri yazılırken hata: {e}", exc_info=True)
