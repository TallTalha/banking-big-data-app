# odeme_kanali_app/producer/simulator.py
"""
Bu modül, QRCode ve Sanal Kart Kullanım Analizi projesi için
gerçekçi, sahte kredi kartı kullanım logları üretmekten sorumludur.

Fonksiyonlar:
    create_user_pool: Analizlerde tutarlılık için sabit bir kullanıcı havuzu oluşturur.
    generate_payment_transaction: Bu havuzu kullanarak tek bir işlem kaydı üretir.
"""
import logging
import random
import uuid
from datetime import datetime, timezone, timedelta

from faker import Faker

LOG = logging.getLogger(__name__)
faker = Faker("tr_TR")

PAYMENT_TYPES = {
    "CHIP":1000,
    "QR":1001,
    "MAIL_ORDER":2000,
    "VISUAL_CARD":2001
}

PAYMENT_WEIGHTS = {
    "CHIP":60,
    "QR":10,
    "MAIL_ORDER":25,
    "VISUAL_CARD":5
}

def create_user_pool(user_count: int) -> list:
    """
    Açıklama:
        user_count girdisi kadar kullanıcıı oluşturur, her kullanıcı eşsiz  oid ve name değerine sahiptir. Kullanıcıları liste olarak döndürür.
    Args:
        user_count(int): Üretilecek eşsiz kullanıcı sayısı.
    Returns:
        user_pool(list): Bütün indeksleri {"oid","name"} tipte olan kullanıcı listesi.
    """
    LOG.info(f"{user_count} adet eşşiz kullanıcı bilgileri içeren havuzu oluşturuluyor.")
    user_pool = []
    for _ in range(user_count):
        user_pool.append(
            {
                "oid": str(faker.random_number(digits=12, fix_len=True)),
                "name": faker.name()
            }
        )
    LOG.info("Kullanıcı havuzu oluşturuldu.")
    return user_pool

# _yardımcı_fonksiyon
def _generate_timestamp_iso() -> str:
    """
    Açıklama:
        Bu yılın başından bugüne kadarki zaman aralığında, rastgele zaman damgaları üretir.
    Args:
        None
    Returns:
        timestamp(str): '%Y-%m-%dT%H:%M:%SZ' formatındaki zaman damgasıdır.
    """
    end_date = datetime.now(timezone.utc)
    start_date = datetime(end_date.year, 1, 1, tzinfo=timezone.utc)
    random_date = faker.date_between_dates(date_start=start_date, date_end=end_date)
    return random_date.strftime('%Y-%m-%dT%H:%M:%SZ')

def generate_payment_transaction(user_pool: list) -> dict:
    """
    Açıklama:
        Girdideki kullanıcı havuzundan rastgele kullanıcılar seçer ve kullanıcıya özel transaction sözlüğü döndürür.
    Args:
        user_pool(list): {"oid","name"} indeksleri içeren kullanıcı listesi.
    Returns:
        transaciton(dict): Bu {"pid","timestamp","oid","name","balance","btype","ptype"} sözlüğü döndürür.
    """ 
    user = random.choice(user_pool)

    chosen_paymnet_method = random.choices(
        list(PAYMENT_WEIGHTS.keys()),
        weights=list(PAYMENT_WEIGHTS.values()),
        k=1
    )[0]

    ptype_code = PAYMENT_TYPES[chosen_paymnet_method]

    transaction = {
        "pid" : str(uuid.uuid4()),
        "timestamp" : _generate_timestamp_iso(),
        "oid" : user["oid"],
        "name" : user["name"],
        "balance" : str(round(random.uniform(10.0, 50.000),2)),
        "btype": random.choice(["TL","EUR","USD"]),
        "ptype": ptype_code
    }
    return transaction