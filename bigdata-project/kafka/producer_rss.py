# ============================================================================
# producer_rss.py -- Kafka Producer untuk Berita Pasar Modal (RSS)
# ============================================================================
# Skrip ini mengambil berita terbaru dari RSS feed pasar modal Indonesia,
# lalu mengirimkan datanya ke Apache Kafka topic 'saham-rss'.
#
# DUAL-WRITE: Data juga disimpan ke file lokal (dashboard/data/live_rss.json)
# sehingga dashboard tetap bisa menampilkan berita meskipun Docker/Kafka mati.
#
# Topik ETS  : Topik 4 -- Monitor Saham IDX
# Kafka Topic: saham-rss
# ============================================================================

import feedparser
import hashlib
import json
import time
import logging
from pathlib import Path
from datetime import datetime

# Kafka bersifat opsional -- bisa jalan tanpa Docker
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
KAFKA_TOPIC = "saham-rss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
RSS_FEEDS = [
    "https://rss.bisnis.com/feed/rss2/financial-market",
    "https://www.cnnindonesia.com/ekonomi/rss",
]
POLLING_INTERVAL_SECONDS = 300  # 5 menit

# Path file lokal untuk dual-write
BASE_DIR = Path(__file__).parent.parent
LIVE_RSS_FILE = BASE_DIR / "dashboard" / "data" / "live_rss.json"

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def get_hash(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()[:8]


def send_to_kafka(producer, data):
    producer.send(KAFKA_TOPIC, key=data["hash"], value=data)
    logger.info("Artikel terkirim ke Kafka: %s", data['title'][:60])


# ---------------------------------------------------------------------------
# Dual-Write: Simpan data berita ke file lokal
# ---------------------------------------------------------------------------
def save_to_local(articles: list[dict]):
    """
    Simpan semua artikel ke live_rss.json.
    Data di-merge dengan file yang sudah ada (deduplikasi berdasarkan hash).
    Maksimal 200 artikel disimpan.
    """
    MAX_ARTICLES = 200
    existing = []
    existing_hashes = set()

    if LIVE_RSS_FILE.exists():
        try:
            with open(LIVE_RSS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            existing = raw if isinstance(raw, list) else raw.get("articles", [])
            existing_hashes = {a.get("hash", "") for a in existing}
        except (json.JSONDecodeError, Exception):
            existing = []

    # Tambahkan artikel baru (skip duplikat)
    new_count = 0
    for article in articles:
        if article.get("hash", "") not in existing_hashes:
            # Transformasi format agar sesuai dengan yang dashboard harapkan
            enriched = {
                "judul": article.get("title", ""),
                "sumber": _extract_source(article.get("link", "")),
                "url": article.get("link", ""),
                "waktu_publikasi": article.get("published", datetime.now().strftime("%Y-%m-%d %H:%M")),
                "ringkasan": _clean_summary(article.get("summary", "")),
                "hash": article.get("hash", ""),
                "keyword_terdeteksi": _detect_keywords(
                    article.get("title", "") + " " + article.get("summary", "")
                ),
            }
            existing.append(enriched)
            existing_hashes.add(article.get("hash", ""))
            new_count += 1

    # Trim ke MAX_ARTICLES terbaru
    if len(existing) > MAX_ARTICLES:
        existing = existing[-MAX_ARTICLES:]

    # Pastikan directory ada
    LIVE_RSS_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(LIVE_RSS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    logger.info("Disimpan ke lokal: %s (%d total, +%d baru)", LIVE_RSS_FILE.name, len(existing), new_count)


def _extract_source(url: str) -> str:
    """Ekstrak nama sumber dari URL."""
    if "bisnis.com" in url:
        return "Bisnis.com"
    elif "cnnindonesia" in url:
        return "CNN Indonesia"
    elif "cnbcindonesia" in url:
        return "CNBC Indonesia"
    elif "kontan" in url:
        return "Kontan.co.id"
    elif "detik" in url:
        return "Detik Finance"
    elif "kompas" in url:
        return "Kompas.com"
    else:
        return "Media Online"


def _clean_summary(summary: str) -> str:
    """Bersihkan HTML tags dari summary RSS."""
    import re
    clean = re.sub(r'<[^>]+>', '', summary)
    clean = clean.strip()
    return clean[:300] if len(clean) > 300 else clean


SAHAM_KEYWORDS = {
    "BCA": ["BCA", "BBCA", "Bank Central Asia"],
    "BRI": ["BRI", "BBRI", "Bank Rakyat Indonesia"],
    "Telkom": ["Telkom", "TLKM", "Telekomunikasi"],
    "Astra": ["Astra", "ASII", "Astra International"],
    "Mandiri": ["Mandiri", "BMRI", "Bank Mandiri"],
}


def _detect_keywords(text: str) -> list[str]:
    """Deteksi keyword saham yang disebutkan dalam teks."""
    found = []
    text_lower = text.lower()
    for keyword, variants in SAHAM_KEYWORDS.items():
        for variant in variants:
            if variant.lower() in text_lower:
                found.append(keyword)
                break
    return found


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("   Memulai Producer RSS Berita Pasar Modal")
    logger.info("   Topic   : %s", KAFKA_TOPIC)
    logger.info("   Feeds   : %d sumber", len(RSS_FEEDS))
    logger.info("   Interval: %d detik", POLLING_INTERVAL_SECONDS)
    logger.info("   File    : %s", LIVE_RSS_FILE)
    logger.info("=" * 60)

    # --- Buat Kafka Producer (opsional) ---
    producer = None
    kafka_mode = False
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8'),
            )
            kafka_mode = True
            logger.info("[OK] Kafka Producer terhubung")
        except Exception as e:
            logger.warning("Kafka tidak tersedia: %s", e)
            logger.info("Berjalan dalam mode LOCAL-ONLY")
    else:
        logger.info("kafka-python tidak terinstall, berjalan dalam mode LOCAL-ONLY")

    sent_hashes = set()
    cycle = 0

    try:
        while True:
            cycle += 1
            logger.info("--- Siklus RSS #%d ---", cycle)

            cycle_articles = []  # kumpulkan untuk simpan lokal

            for rss_url in RSS_FEEDS:
                try:
                    feed = feedparser.parse(rss_url)
                    logger.info("Mengambil %d artikel dari %s", len(feed.entries), rss_url)

                    for entry in feed.entries:
                        h = get_hash(entry.link)
                        if h in sent_hashes:
                            continue

                        data = {
                            "title": entry.get("title", ""),
                            "link": entry.get("link", ""),
                            "summary": entry.get("summary", ""),
                            "published": entry.get("published", ""),
                            "hash": h,
                        }

                        # Kirim ke Kafka jika tersedia
                        if kafka_mode and producer:
                            try:
                                send_to_kafka(producer, data)
                            except Exception as e:
                                logger.error("Gagal kirim ke Kafka: %s", e)

                        cycle_articles.append(data)
                        sent_hashes.add(h)

                except Exception as e:
                    logger.error("Error parsing RSS %s: %s", rss_url, e)

            # Flush Kafka
            if kafka_mode and producer:
                producer.flush()

            # Dual-Write: simpan ke file lokal
            if cycle_articles:
                save_to_local(cycle_articles)

            logger.info(
                "Siklus #%d selesai --- %d artikel baru ditemukan",
                cycle, len(cycle_articles),
            )

            logger.info(
                "Menunggu %d detik sebelum polling berikutnya...\n",
                POLLING_INTERVAL_SECONDS,
            )
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("\nProducer RSS dihentikan oleh pengguna (Ctrl+C)")

    finally:
        if producer:
            producer.flush()
            producer.close()
        logger.info("[OK] Producer RSS ditutup dengan aman. Selesai.")


if __name__ == "__main__":
    main()