# producer/eft-havale-producer.py
"""
Bu script, iş akışındaki veritabanı verilerini simüle eder. Para transfer işleminde
oluşabilecek verileri, standart bir formatta, eşşiz id alanları ve rastgele bilgilerle 
JSON objesi üretir. 
"""
import json
import random
import os
import sys
import uuid
from time import sleep
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from utils.logger import setup_logger
from configs.settings import KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC, TRANSACTION_COUNT, USER_COUNT

LOG = setup_logger("eftHavaleProducer")
faker = Faker("tr_TR")
BANKS = ["İş","Garanti","Kuveyt","Şeker","Ziraat","Vakıf"]

def create_user_pool(user_count: int) -> list:
    """
    Belirtilen sayıda, tutarlı sahte kullanıcı profili oluşturur, her kullanıcı 2-4 arasında banka hesabına sahhiptir.
    Sahte kullanıcı havuzu list olarak döndürülür.
        Args:
            user_count(int): Üretilecek kullanıcı sayısı.
        Returns:
            pool(list): Sahte kullanıcı verileri içeren liste.
    """
    LOG.info(f"{user_count} adet sahte kullanıcı oluşturuluyor...")
    
    pool = []
    for _ in range(user_count):
            
        bank_acc_pool = []
        acc_count = random.randint(2,4)
        for _ in range(acc_count):
            bank_acc_pool.append(
                    {
                        "bank": f"{random.choice(BANKS)} Bankası",
                        "iban": faker.iban()
                    }
            )

        pool.append(
            {
                "oid": faker.random_number(digits=12, fix_len=True),
                "name": faker.name(),
                "bank_accounts": bank_acc_pool
            }
        )
    
    LOG.info("Müşteri havuzu oluşturuldu.")
    return pool

def generate_transactions(user_pool: list) -> dict:
    """
    Kullanıcı listesini kullanarak, ortam değiskeninde belirtilen adet kadar sahte transaction verisi üretir ve dict olarak döndürür.  
        Args:
            user_pool(list): oid,name,iban değerlerine sahip sözlükleri içeren kullanıcı listesi. 
        Returns:
            transactions(dict): pid,ptype,account(oid,name,iban),info(name,iban,bank),balance,btype değerlerini içeren transaction sözlük yapısını döndürür.
    """
    sender = random.choice(user_pool)
    sender_bank_acc = random.choice(sender["bank_accounts"])

    # 1000 kullanıcı arasında aynı kullanıcıyı ve aynı kullanıcının aynı ibanını seçmek çok düşük bir ihtimal
    # while(true) yerine maksimum 10 kere dönen döngü kullandım. Sonuç olarak ihtimal dahilinde ama sahte veri olduğu için
    # mükemelliyetçilik yapmaya gerek yok
    for _ in range(10): 
        receiver = random.choice(user_pool)
        receiver_bank_acc = random.choice(receiver["bank_accounts"])

        if sender["oid"] == receiver["oid"] and sender_bank_acc["iban"] == receiver_bank_acc["iban"]: 
            continue
        else:
            break

    transaction = {
        "pid": str(uuid.uuid4()),
        "ptype": random.choice(["E","H"]),
        "account":{
            "oid": sender["oid"],
            "name": sender["name"],
            "iban": sender_bank_acc["iban"]
        },
        "info":{
            "name": receiver["name"],
            "iban": receiver_bank_acc["iban"],
            "bank": receiver_bank_acc["bank"]
        },
        "balance": str(round(random.uniform(10.0, 25000.0), 2)),
        "btype": "TL" 
    }   #BTYPE değeri TL olmak zorunda ya da iban'ların bitim tipi olmalı 
        #ve rastgele seçim yapılırken aynı tip olması kontrol edilmeli
    return transaction

def create_kafka_producer() -> KafkaProducer | None:
    """
    Bu fonksiyon kafka producer nesnesi oluşturur.
        Args:
            None
        Returns:
        producer(KafkaProducer): Kafka producer nesnesi.
    """

    try:
        producer= KafkaProducer(
            bootstrap_servers = KAFKA_BOOTSTRAPSERVERS,
            value_serializer = lambda v: json.dumps(v).encode("utf-8")
        )
        LOG.info("Kafka Producer başarıyla oluşturuldu.")
        return producer
    except KafkaError as ke:
        LOG.critical(f"Kafka Producer nesnesi oluşturulurken hata: {ke}", exc_info=True)
        return None
    
def main():
    """
    Ana iş akışını düzenler.
    """
    producer = create_kafka_producer()

    if not producer:
        print("Producer is None->sys.exit(1)")
        sys.exit(1) # ÇIKIŞ -> Kafka Nesnesi oluşmadı.

    user_pool = create_user_pool(USER_COUNT)

    LOG.info(f"{KAFKA_TOPIC} topiğine, {TRANSACTION_COUNT} adet transaction gönderiliyor.")
    
    try:
        for i in range(TRANSACTION_COUNT):
            transaction = generate_transactions(user_pool)
            producer.send(KAFKA_TOPIC, value=transaction)

            if (i+1)%500 == 0:
                LOG.info(f"{i+1} adet veri gönderildi.")
            
            sleep(0.005) # 5ms bekle
    except Exception as e:
        LOG.error(f"Kafka Topiğine veri gönderirken hata:{e}", exc_info=True)

    finally:
        LOG.info("(flush) Tüm mesajların gönderilmesi bekleniyor.")
        producer.flush()
        producer.close()
        LOG.info("Kafka Producer kapatıldı. Producer Scripti tamamlandı.")
    