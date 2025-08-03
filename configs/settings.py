# configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Ortam Değişkenleri
KAFKA_BOOTSTRAPSERVERS = str(os.getenv("KAFKA_BOOTSTRAPSERVERS"))
KAFKA_TOPIC = str(os.getenv("KAFKA_TOPIC"))
ODEME_KANALI_KAFKA_TOPIC = str(os.getenv("ODEME_KANALI_KAFKA_TOPIC"))

#MongoDB
MONGO_URI = str(os.getenv("MONGO_URI"))
MONGO_DB_DWH = str(os.getenv("MONGO_DB_DWH"))
MONGO_COLLECTION_DWH = str(os.getenv("MONGO_COLLECTION_DWH"))

# PostgreSQL
POSTGRES_USER = str(os.getenv("POSTGRES_USER")) 
POSTGRES_PASSWORD = str(os.getenv("POSTGRES_PASSWORD")) 
POSTGRES_DB = str(os.getenv("POSTGRES_DB")) 
POSTGRES_TABLE = str(os.getenv("POSTGRES_TABLE")) 

# Transaction Ayarları
USER_COUNT = int(os.getenv("USER_COUNT")) #type: ignore
TRANSACTION_COUNT = int(os.getenv("TRANSACTION_COUNT")) #type: ignore

ODEME_KANALI_USER_COUNT = int(os.getenv("ODEME_KANALI_USER_COUNT")) #type: ignore
ODEME_KANALI_TRANSACTION_COUNT = int(os.getenv("ODEME_KANALI_TRANSACTION_COUNT")) #type: ignore