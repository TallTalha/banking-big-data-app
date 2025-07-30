# utils/logger.py
"""
Bu modül, uygulama genelinde kullanılacak logger'ı ayarlar.
Logger, hem konsola hem de dosyaya loglama yapar.
Log dosyaları, uygulamanın kök dizinindeki logs klasöründe saklanır.
Log dosyaları, modül adıyla adlandırılır ve her modül için ayrı bir log dosyası oluşturulur.
Loglama formatı, tarih, modül adı, log seviyesi ve mesajı içerir.
Log dosyaları, 10 MB boyutuna ulaştığında yeni bir dosya oluşturur ve en fazla 5 yedek dosya tutar. 
"""
import logging
import logging.handlers
import os
import sys
import re

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_ROOT = os.path.join(PROJECT_ROOT,"logs")
os.makedirs(LOG_ROOT, exist_ok=True)

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Logger'ı ayarlar ve döndürür. Her logger, log dosyasında kendi modül adıyla ayrı bir dosyaya yazılır.
    Örneğin. main.py modülü için logs/main.log dosyasına yazılır. 10 MB boyutuna ulaştığında yeni bir dosya oluşturur,
    en fazla 5 yedek dosya tutar
        Args:
            name (str): Logger'ın adı, genellikle modül adı olarak kullanılır.
            level (int): Log seviyesini belirler. Varsayılan olarak INFO seviyesidir.
        Returns:
            logging.Logger: Ayarlanmış logger nesnesi.
    """
    safe_name = re.sub(r"[^\w\-_.]", "_", name)
    LOG_FILE = os.path.join(LOG_ROOT,f"{safe_name}.log")
    
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s")

    fileHandler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10*1024*1024, backupCount=5
    ) 
    fileHandler.setFormatter(formatter)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)

    logger = logging.getLogger(name=name)
    logger.setLevel(level=level)

    if not logger.handlers:
        logger.addHandler(fileHandler)
        logger.addHandler(consoleHandler)
    
    return logger

