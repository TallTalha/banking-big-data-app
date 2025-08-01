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

#MongoDB
MONGO_URI = str(os.getenv("MONGO_URI")) 

# Transaction Ayarları
USER_COUNT = int(os.getenv("USER_COUNT")) #type: ignore
TRANSACTION_COUNT = int(os.getenv("TRANSACTION_COUNT")) #type: ignore