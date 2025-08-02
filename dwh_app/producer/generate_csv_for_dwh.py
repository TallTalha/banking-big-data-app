# dwh_app/producer/generate_csv_for_dwh.py
"""
Bu script, eft_havale_app/producer/data_generator.py'deki veri üretme fonksiyonlarını kullanarak,
DWH (Veri Ambarı) projesinde PostgreSQL'e yüklenecek olan,
büyük hacimli bir CSV dosyası oluşturur.
"""
import csv
import os
import sys
from eft_havale_app.producer.data_generator import generate_transactions, create_user_pool, generate_random_timestamp_iso

current_dir = os.path.dirname(os.path.abspath(__file__)) # .../producer
app_root_dir = os.path.dirname(current_dir)             # .../dwh_app
project_root = os.path.dirname(app_root_dir)            # .../banking-big-data-app
sys.path.append(project_root)

import logging
from utils.logger import set_logger

set_logger("generate_csv_for_dwh",app_file_path=app_root_dir)
LOG = logging.getLogger(__name__)

OUTPUT_CSV_FILE = "eft_havale_transactions.csv"
NUM_RECORDS = 30000
USER_POOL_SIZE = 1000

def flatten_transaction(nested_transaction: dict) -> dict:
    """
    Açıklama:
        generate_transaction'dan gelen iç içe geçmiş sözlüğü,
        CSV için uygun olan düz bir yapıya dönüştürür.
    Args:
        nested_transaction(dict): eft_havale_app.producer.data_generator modülünün ürettiği içe içe veri yapısıdır.
    Returns:
        flat_dict(dict): Girdideki sözlük yapısının, içe içe  yapılarını, düz sözlük yapısına dönüştürüldüğü yeni sözlük yapısıdır.
    """
    return {
        "pid": nested_transaction["pid"],
        "timestamp": nested_transaction["timestamp"] ,
        "ptype": nested_transaction["ptype"],
        "sender_oid": nested_transaction["account"]["oid"],
        "sender_name": nested_transaction["account"]["name"],
        "sender_iban": nested_transaction["account"]["iban"],
        "receiver_name": nested_transaction["info"]["name"],
        "receiver_iban": nested_transaction["info"]["iban"],
        "receiver_bank": nested_transaction["info"]["bank"],
        "balance": nested_transaction["balance"],
        "btype": nested_transaction["btype"]
    }

def main():
    """Ana iş akışını yönetir."""
    LOG.info(f"{NUM_RECORDS} adet kayıttan oluşacak '{OUTPUT_CSV_FILE}' dosyası hazırlanıyor...")
    user_pool = create_user_pool(user_count=USER_POOL_SIZE)

    field_names = [
        "pid", "timestamp", "ptype", "sender_oid", "sender_name", "sender_iban",
        "receiver_name", "receiver_iban", "receiver_bank", "balance", "btype"
    ]

    try:

        with open(OUTPUT_CSV_FILE, 'w', newline='',encoding='utf-8') as csvfile:
            
            writer = csv.DictWriter(csvfile, fieldnames=field_names)

            writer.writeheader()

            for i in range(NUM_RECORDS):
                nested_transactions= generate_transactions(user_pool=user_pool, timestamp_str= generate_random_timestamp_iso())
                flat_dict = flatten_transaction(nested_transaction=nested_transactions)
                writer.writerow(flat_dict)

                if (i+1)%1000 == 0:
                    LOG.info(f'{i+1} satır CSV dosyasına yazıldı.')
        
        LOG.info(f'{OUTPUT_CSV_FILE} başarıyla oluşturuldu.')
    except Exception as e:
        LOG.critical(f'{OUTPUT_CSV_FILE} oluşturulurken hata:{e}', exc_info=True)

if __name__ ==  '__main__':
    main()