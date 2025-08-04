# odeme_kanali_app/consumer/analysis_logic.py
"""
Bu modül, Spark DataFrame'leri üzerinde, ödeme kanalı analizi projesine
özel iş mantığını ve analizleri içeren fonksiyonları barındırır.
"""
import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

LOG = logging.getLogger(__name__)

def pivot_payment_types(df: DataFrame) -> DataFrame:
    """
    Açıklama:
        Kullanıcı bazında, her bir ödeme tipinin kaç kez kullanıldığını gösteren
        bir pivot tablo oluşturur.
    Args:
        df(DataFrame): ["pid","timestamp","oid","name","balance","btype","ptype"] bu kolonlara sahip transaction tablosu.
    Returns:
        pivot_df(DataFrame): ["ptype"] değerleriyle, ["oid", "name", "1000", "1001", "2000", "2001"] kullanıcıların hangi ödeme kanallarını kullandığını gösteren tabloyu döndürür. 
    """
    LOG.info("Ödeme tipleri pivot tablosu oluşturuluyor...")

    pivot_df = (
        df.groupBy(F.col("oid"),F.col("name"))
        .pivot("ptype")
        .count()
        .na.fill(0) # Null değerleri 0 ile doldur
    )
    return pivot_df

def find_qr_code_candidates(pivot_df: DataFrame, chip_threshold: float = 5.0) -> DataFrame :
    """
    Açıklama:
        QRCode kampanyası için hedef kitleyi belirler.
        Belirli bir sayının üzerinde çipli kart kullanan ama HİÇ QR kod kullanmayanları bulur.
    Args:
        pivot_df(DataFrame): ["oid", "name", "1000", "1001", "2000", "2001"] kolonlarına sahip ödeme kanalı pivot tablosu.
        chip_thresold(int): Müşterinin çipli ödeme eşiği. Bu eşiğin üstünde çipli ödeme yapan müşteriler referans alınacak.
    Returns:
        qr_candidates_df(DataFrame): chip_thresold limitinden fazla çipli ödeme yapmış ve hiç qr code ile ödeme yapmamış kullanıcıların tablosu.  
    """
    LOG.info("QR Code Kampanyası için hedef kitle tablosu oluşturuluyor...")

    qr_candidates_df = (
        pivot_df.filter(
            (F.col("1000") > F.lit(chip_threshold) ) & (F.col("1001") == 0)
        )
    )

    return qr_candidates_df

def find_visual_card_candidates(pivot_df: DataFrame, mail_order_threshold: float = 5.0) -> DataFrame:
    """
    Açıklama:
        Sanal Kart kampanyası için hedef kitleyi belirler.
        Belirli bir sayının üzerinde mail order kullanan ama HİÇ sanal kart kullanmayanları bulur.
    Args:
        pivot_df(DataFrame): ["oid", "name", "1000", "1001", "2000", "2001"] kolonlarına sahip ödeme kanalı pivot tablosu.
        mail_order_threshold(int): Müşterinin mail order ile ödeme eşiği. Bu eşiğin üstünde çipli ödeme yapan müşteriler referans alınacak.
    Returns:
        visual_card_candidates_df(DataFrame): chip_thresold limitinden fazla çipli ödeme yapmış ve hiç qr code ile ödeme yapmamış kullanıcıların tablosu.  
    """
    LOG.info(f"Sanal Kart hedef kitlesi aranıyor (Mail Order > {mail_order_threshold}, Sanal Kart = 0)...")
    
    visual_card_candidates_df = (
        pivot_df.filter(
            (F.col("2000") > F.lit(mail_order_threshold) ) & (F.col("2001") == 0)
        )
    )

    return visual_card_candidates_df

    