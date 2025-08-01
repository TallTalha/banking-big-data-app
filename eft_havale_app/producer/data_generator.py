# eft_havale_app/data_generator.py
"""
Bu script, iş akışındaki veritabanı verilerini simüle eder. Para transfer işleminde
oluşabilecek verileri, standart bir formatta, eşşiz id alanları ve rastgele bilgilerle 
JSON objesi üretir. 
"""
import random
from faker import Faker
import uuid
from datetime import datetime, timezone, timedelta
import calendar
import logging

LOG = logging.getLogger(__name__)

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

# Batch analiz için 2024 yılı için zaman damgası üretir:
def generate_random_timestamp_iso() -> str:
    """
    Rastgele bir ISO 8601 zaman damgası üretir.
        Args:
            None
        Returns:
            str: Rastgele üretilmiş ve %Y-%m-%dT%H:%M:%SZ formatındaki zaman damgası.
    """
    year = 2024
    month = random.randint(1, 12)

    # Belirtilen yıl ve ay için geçerli gün sayısını al
    _, max_day = calendar.monthrange(year, month)
    day = random.randint(1, max_day)

    # Gün içindeki rastgele saniye
    seconds = random.randint(0, 24 * 60 * 60 - 1)

    # Başlangıç datetime nesnesi (sıfır saatli)
    base = datetime(year, month, day, tzinfo=timezone.utc)

    # O güne saniyeleri ekle
    random_dt = base + timedelta(seconds=seconds)

    return random_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# real-time analiz için anlık zaman damgası üretir:
def current_timestamp_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

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
        "timestamp":generate_random_timestamp_iso(),
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