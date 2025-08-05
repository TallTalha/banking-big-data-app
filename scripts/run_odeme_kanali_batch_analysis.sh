#!/bin/bash
set -e

# YENİ EKLENEN SATIR: Spark ortam değişkenlerini manuel olarak yükle
source /etc/profile.d/spark.sh

echo "ODEME KANALI BATCH İşi Başlatılıyor..."

# Projenin kök dizinine git (script'in konumuna göre)
cd "$(dirname "$0")/.."

# Gerekli ortam değişkenlerini ayarla
export PYSPARK_PYTHON=./odeme_kanali_app/consumer/venv-consumer/bin/python
export PYTHONPATH=$(pwd)

# Spark işini gönder
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  odeme_kanali_app/consumer/run_batch_analysis.py

echo "ODEME KANALI BATCH İşi Tamamlandı."
