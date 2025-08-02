# dwh_app/consumer/main.py
"""
Bu script; 
    1.  PostgreSQL'den verileri çeker.
    2.  Veri şemasını ayarlar.
    3.  Verileri MongoDB'ye yazar.
"""
from dotenv import load_dotenv 
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__)) # .../consumer
app_root_dir = os.path.dirname(current_dir)             # .../dwh_app
project_root = os.path.dirname(app_root_dir)            # .../banking-big-data-app
load_dotenv(dotenv_path=os.path.join(project_root, ".env"))
sys.path.append(project_root)

from configs.settings import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_TABLE, MONGO_COLLECTION_DWH, MONGO_DB_DWH

import logging
from utils.logger import set_logger

set_logger("consumer_main",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

from data_consumer import create_spark_session, read_from_postgres, write_to_mongo
from data_transformer import transform_data

def main():
    """
    Ana iş akışını yönetir.
    """
    spark = create_spark_session(appName="rdb_to_mongo")
    if not spark:
        sys.exit(1) # Çıkış: Spark Session Oluşturulamadı.

    df = read_from_postgres(
        spark=spark,
        p_db=POSTGRES_DB,
        p_table=POSTGRES_TABLE,
        p_user=POSTGRES_USER, 
        p_password=POSTGRES_PASSWORD
    )
    if df is None or df.isEmpty():
        LOG.info("PostgreSQL'de veri bulunamadı. İşlem sonlandırıldı.")
        sys.exit(1) # Çıkış: İşlenecek veri yok.

    transformed_df = transform_data(df=df)
    transformed_df.cache()

    write_to_mongo(df=transformed_df, db=MONGO_DB_DWH, collection=MONGO_COLLECTION_DWH)

if __name__ == "__main__":
    main()

    