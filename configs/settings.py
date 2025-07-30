# configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Ortam Değişkenleri
KAFKA_BOOTSTRAPSERVERS = os.getenv("KAFKA_BOOTSTRAPSERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Transaction Ayarları
USER_COUNT = os.getenv("USER_COUNT") 
TRANSACTION_COUNT = os.getenv("TRANSACTION_COUNT")