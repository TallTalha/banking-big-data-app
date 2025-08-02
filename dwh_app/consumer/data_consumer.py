# eft_havale_app/consumer/consumer.py
"""
Bu modül, PostgreSQL fen veri çekmek ve MongoDB'ye yazmak için gerekli fonksiyonalrı içerir.  
"""
from pyspark.sql import SparkSession, DataFrame

import logging

LOG = logging.getLogger(__name__)

def create_spark_session(appName: str) -> SparkSession | None :
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
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        LOG.info("Spark Session başarıyla oluşturuldu.")
        return spark
    except Exception as e:
        LOG.critical("Spark Session oluştururken hata: {e}", exc_info=True)
        return None

def read_from_postgres(spark: SparkSession, p_db: str, p_table: str, p_user: str, p_password: str) -> DataFrame | None :
    """
    Açıklama:
        PostgreSQL veritabanından bir tabloyu Spark DataFrame olarak okur.
    Args:
        spark(SparkSession): Spark uygulaması için aktif oturum nesnesi.
        p_db(str): Bağlanılacak PostgreSQL veritabanının adı.
        p_table(str): Okunacak tablo adı (şema_adı.tablo_adı şeklinde tam olarak verilmelidir).
        p_user(str): Veritabanı kullanıcı adı.
        p_password(str): Veritabanı şifresi.
    Returns:
        df(DataFrame | None): Belirtilen PostgreSQL tablosu başarıyla okunursa Spark DataFrame döner, bağlantı hatası vb. durumlarda None dönebilir.
    Not:
        -   Spark JDBC kullanılarak bağlantı kurulur.
        -   Bağlantı için varsayılan olarak localhost:5432 adresi ve PostgreSQL JDBC sürücüsü kullanıldığı varsayılır.
        -   Şema adı tablo adından ayrı olarak verilmeliyse `dbtable="şema.tablo"` formatı kullanılmalıdır.
    """
    LOG.info(f"PostgreSQL'den veri okunuyor: {p_db}.{p_table}")
    try:
        df = (
            spark.read
            .format("jdbc")
            .option("url",f"jdbc:postgresql://localhost:5432/{p_db}")
            .option("dbtable", p_table)
            .option("user", p_user)
            .option("password", p_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        LOG.info("PostgreSQL'den veriler başarıyla okundu.")
        return df
    except Exception as e:
        LOG.critical("PostgreSQL'den veriler okunurken hata: {e}", exc_info=True)
        return None
    
def write_to_mongo(df: DataFrame, db: str, collection: str) -> None:
    """
    Açıklama:
        Bir Spark Batch DataFrame'ini belirtilen MongoDB koleksiyonuna yazar.
        Varolan verinin üzerine yazar (overwrite)
    Args:
        df(DataFrame): Mongo'ya yazılacak veri yapısı.
        db(str): Verinin yazılacağı koleksiyonun bulunduğu veritabanı ismi.
        collection(str): Verinin yazılacağı koleksiyon ismi.
    Returns:
        None
    """
    LOG.info(f"{collection} koleksiyonuna BATCH veri yazılıyor...")
    try:
        (
            df.write.format("mongodb") 
            .mode("overwrite") 
            .option("uri", f"mongodb://localhost:27017/{db}") 
            .option("collection", collection) 
            .save()
        )
            
        LOG.info("Veri MongoDB'ye başarıyla yazıldı.")
    except Exception as e:
        LOG.critical(f"{collection} koleksiyonuna BATCH veri yazılırken hata: {e}", exc_info=True)

        