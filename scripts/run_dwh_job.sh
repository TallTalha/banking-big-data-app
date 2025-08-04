#!/bin/bash
set -e

# YENİ EKLENEN SATIR: Spark ortam değişkenlerini manuel olarak yükle
source /etc/profile.d/spark.sh

echo "DWH ETL İşi Başlatılıyor..."

# Projenin kök dizinine git (script'in konumuna göre)
cd "$(dirname "$0")/.."

# Gerekli ortam değişkenlerini ayarla
export PYSPARK_PYTHON=./dwh_app/consumer/venv-consumer/bin/python
export PYTHONPATH=$(pwd)

# Spark işini gönder
spark-submit \
  --packages org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  dwh_app/consumer/main.py

echo "DWH ETL İşi Tamamlandı."



