from data_transformer import transform_transactions
from data_consumer import create_spark_session, read_from_kafka
from configs.settings import KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC
from pyspark.sql import functions as F

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__)) # .../consumer
app_root_dir = os.path.dirname(current_dir)             # .../eft_havale_app
project_root = os.path.dirname(app_root_dir)            # .../banking-big-data-app
sys.path.append(project_root)

import logging
from utils.logger import set_logger

set_logger("consumer_stream_analysis",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

def main():
    """
    Açıkklama:
        Real-time verilerin işlenmesinde iş akışını düzenler.
    """ 
