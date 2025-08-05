# Finans Sektörü İçin Büyük Veri Boru Hatları Projesi 🏦

*Bu proje, finans sektöründe karşılaşılan yaygın senaryoları (EFT takibi, Veri Ambarı modernizasyonu, müşteri segmentasyonu) modelleyen, modern veri mühendisliği araçları ve best practice'ler kullanılarak geliştirilmiş, uçtan uca veri boru hatları koleksiyonudur.*

-----

## 1\. Projenin Genel Felsefesi ve Best Practice'ler

Bu repo, tek bir uygulamadan çok, her biri kendi özel problemini çözen, ancak ortak profesyonel prensipler üzerine inşa edilmiş bir dizi veri mühendisliği uygulamasını barındırır. Proje genelinde aşağıdaki yaklaşımlar benimsenmiştir:

  * **Modüler Mimari:** Proje, her biri kendi klasöründe yönetilen, bağımsız çalışabilen alt uygulamalara (`eft_havale_app`, `dwh_app`, `odeme_kanali_app`) bölünmüştür.
  * **Sorumlulukların Ayrılığı (SoC):** Her alt uygulama, kendi içinde veri üretme (`producer`), veri işleme (`consumer`), konfigürasyon (`configs`) ve yardımcı araçlar (`utils`) gibi mantıksal katmanlara ayrılmıştır.
  * **İzole Ortamlar:** Her bir bağımsız bileşen, kendi Python sanal ortamına (`venv`) ve `requirements.txt` dosyasına sahiptir. Bu, bağımlılık çakışmalarını önler ve projenin taşınabilirliğini artırır.
  * **Merkezi Konfigürasyon ve Güvenlik:** Hassas bilgiler (şifreler, sunucu adresleri), `.env` dosyası aracılığıyla koddan tamamen soyutlanmıştır ve `.gitignore` ile versiyon kontrolü dışında tutulmaktadır.
  * **Sağlam (Robust) ve Hata Toleranslı (Fault-Tolerant) Kod:** Tüm dış dünya etkileşimleri (API, veritabanı bağlantıları, dosya işlemleri) `try-except-finally` blokları ile güvence altına alınmıştır. Spark Streaming işlerinde `checkpointing` mekanizması kullanılarak veri kaybı önlenmiştir.
  * **Güvenli Erişim:** Sunucudaki tüm servislere (MongoDB, Elasticsearch, Kibana, Spark UI) dış ağdan erişim kapatılmış, tüm geliştirme ve yönetim işlemleri güvenli bir **SSH Tüneli** üzerinden sağlanmıştır.

## 2\. Kullanılan Ana Teknolojiler

  * **Veri Akışı ve Mesajlaşma:** Apache Kafka (KRaft Modu)
  * **Veri İşleme:** Apache Spark 3.5.1 (PySpark - Batch & Structured Streaming)
  * **Veri Depolama:**
      * MongoDB (NoSQL - Analiz sonuçları ve esnek veriler için)
      * PostgreSQL (RDB - Geleneksel veri kaynaklarını simüle etmek için)
  * **Veri Üretimi ve Entegrasyon:** Python, Faker
  * **Altyapı ve Yönetim:** DigitalOcean Droplet (Ubuntu - Single Node), `systemd` (Servis yönetimi)
  * **Gözlemlenebilirlik:** Python `logging` modülü

## 3\. Proje Dosya Yapısı

```
banking-big-data-app/
├── configs/
│   └── settings.py               # .env'den ayarları okuyan, TÜM uygulamalar için ortak modül
├── utils/
│   └── logger.py                 # TÜM uygulamalar için merkezi loglama modülü
│
├── eft_havale_app/               # Uçtan uca veri üretimi ve analizi uygulaması
│   ├── producer/
│   └── consumer/
│   └── logs/
│
├── dwh_app/                      # RDB'den NoSQL'e ETL boru hattı uygulaması
│   ├── producer/
│   └── consumer/
│   └── logs/
│
├── odeme_kanali_app/             # Müşteri segmentasyon analizi uygulaması
│   ├── producer/
│   └── consumer/
│   └── logs/
│
├── scripts/                      # spark-submit gibi komutları içeren .sh script'leri
│   └── run_dwh_job.sh
│   └── run_odeme_kanali_batch_analysis.sh
│
├── eft_havale_transactions.csv   # dwh_app için üretilen örnek CSV dosyası
├── .env.example                  # .env dosyasının nasıl olması gerektiğini gösteren şablon
├── .gitignore
└── README.md
```

## 4\. Alt Uygulamalar

### 4.1. `eft_havale_app` - Gerçek Zamanlı İşlem Simülasyonu ve Analizi

  * **Amaç:** Bankalar arası EFT/Havale işlemlerini gerçekçi bir şekilde simüle eden ve bu veriyi Kafka üzerinden Spark ile hem toplu (batch) hem de anlık (streaming) olarak analiz eden bir boru hattı.
  * **Mimari:** `Python Producer -> Kafka -> Spark (Batch & Streaming) -> MongoDB`
  * **Öne Çıkan Özellikler:** `Faker` ile tutarlı kullanıcı havuzları oluşturma, `pivot` ve `window` gibi gelişmiş Spark analiz fonksiyonlarının kullanımı.

### 4.2. `dwh_app` - RDB'den NoSQL'e ETL Projesi

  * **Amaç:** Geleneksel bir ilişkisel veritabanında (PostgreSQL) bulunan yapısal veriyi, Spark kullanarak okumak, dönüştürmek ve modern bir NoSQL veritabanına (MongoDB) yüklemek. Klasik bir Veri Ambarı (DWH) modernizasyon senaryosunu temsil eder.
  * **Mimari:** `PostgreSQL (RDB) -> Spark Batch -> MongoDB (NoSQL)`
  * **Öne Çıkan Özellikler:** Spark'ın JDBC veri kaynağını kullanarak ilişkisel veritabanlarına bağlanması ve `DataFrameWriter` API'si ile NoSQL hedeflerine veri yazması.

### 4.3. `odeme_kanali_app` - Müşteri Segmentasyon Analizi

  * **Amaç:** Kredi kartı kullanım loglarını analiz ederek, bankanın modern ödeme kanallarını (QRCode, Sanal Kart) kullanmayan ancak potansiyeli yüksek olan müşteri segmentlerini tespit etmek.
  * **Mimari:** `Python Producer -> Kafka -> Spark Batch -> MongoDB`
  * **Öne Çıkan Özellikler:** İş probleminden yola çıkarak, `filter` ve `groupBy` gibi temel Spark operasyonları ile hedefe yönelik pazarlama listeleri oluşturma.

## 5\. Kurulum ve Çalıştırma

### Sunucu Gereksinimleri

Projenin çalıştırılacağı sunucuda Java, Python3, `venv`, `git` ve ana servislerin (Kafka, Spark, MongoDB, PostgreSQL) kurulu ve çalışır durumda olması gerekmektedir.

### Yerel Kurulum ve Çalıştırma

1.  **Depoyu Klonlama:** `git clone <proje_url>`
2.  **.env Dosyası:** Proje kök dizininde `.env.example` dosyasını kopyalayarak `.env` adında yeni bir dosya oluşturun ve içindeki hassas bilgileri (şifreler, sunucu adresleri vb.) kendi ortamınıza göre doldurun.
3.  **Bağımlılıklar:** Her bir alt uygulamanın `producer` ve `consumer` klasörlerine girerek, `venv` sanal ortamlarını oluşturun ve `pip install -r requirements.txt` ile bağımlılıkları kurun.
4.  **Çalıştırma:** Her uygulamanın kendi `README.md` dosyasında veya `scripts/` klasöründeki `.sh` dosyalarında detaylı çalıştırma talimatları bulunur. Genel akış, `producer` script'ini çalıştırarak Kafka'yı beslemek ve ardından `consumer` script'ini `spark-submit` ile çalıştırmaktır.