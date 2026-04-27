# ============================================================================
# producer_api.py — Kafka Producer untuk Data Saham IDX (Real-Time Polling)
# ============================================================================
# Skrip ini mengambil harga saham real-time dari Yahoo Finance (yfinance)
# untuk 5 emiten IDX, lalu mengirimkan datanya ke Apache Kafka secara
# periodik setiap 60 detik.
#
# Topik ETS  : Topik 4 — Monitor Saham IDX
# Kafka Topic: saham-api
# ============================================================================

import json
import logging
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "saham-api"
TICKERS = ["BBCA.JK", "BBRI.JK", "TLKM.JK", "ASII.JK", "BMRI.JK"]
POLLING_INTERVAL_SECONDS = 60

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
# Helper: JSON Serializer
# ---------------------------------------------------------------------------
def json_serializer(data: dict) -> bytes:
    """Serialize dictionary ke bytes JSON (UTF-8)."""
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def key_serializer(key: str) -> bytes:
    """Serialize Kafka key (ticker symbol) ke bytes UTF-8."""
    return key.encode("utf-8")


# ---------------------------------------------------------------------------
# Helper: Delivery Callbacks
# ---------------------------------------------------------------------------
def on_send_success(record_metadata):
    """Callback ketika pesan berhasil terkirim ke Kafka."""
    logger.info(
        " Berhasil mengirim ke topic=%s partition=%s offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(excp):
    """Callback ketika pesan gagal terkirim ke Kafka."""
    logger.error(" Gagal mengirim pesan ke Kafka: %s", excp)


# ---------------------------------------------------------------------------
# Core: Inisialisasi Kafka Producer
# ---------------------------------------------------------------------------
def create_producer() -> KafkaProducer:
    """
    Membuat instance KafkaProducer dengan konfigurasi idempotent
    dan acknowledgement penuh untuk menjamin data tidak duplikat
    dan tidak hilang (exactly-once semantics pada level producer).
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=key_serializer,
        # --- Keamanan & Reliabilitas Data ---
        enable_idempotence=True,  # Mencegah duplikasi pesan
        acks="all",               # Menunggu semua replica menulis
        retries=5,                # Retry otomatis jika gagal
        max_in_flight_requests_per_connection=1,  # Menjaga urutan pesan
    )
    logger.info(" Kafka Producer berhasil terhubung ke %s", KAFKA_BOOTSTRAP_SERVERS)
    return producer


# ---------------------------------------------------------------------------
# Core: Ambil Data Saham dari yfinance
# ---------------------------------------------------------------------------
def fetch_stock_data(symbol: str) -> dict | None:
    """
    Mengambil data harga saham terkini dari Yahoo Finance menggunakan
    yf.Ticker().fast_info untuk performa yang lebih cepat.

    Args:
        symbol: Ticker saham (contoh: "BBCA.JK").

    Returns:
        Dictionary berisi metadata saham, atau None jika gagal.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info

        # Ambil harga terakhir dan volume
        last_price = info.get("lastPrice", None) or info.get("last_price", None)
        volume = info.get("lastVolume", None) or info.get("last_volume", None)

        # Fallback: jika fast_info tidak mengembalikan harga, gunakan history
        if last_price is None:
            hist = ticker.history(period="1d")
            if not hist.empty:
                last_price = float(hist["Close"].iloc[-1])
                volume = int(hist["Volume"].iloc[-1])

        if last_price is None:
            logger.warning("  Tidak dapat mengambil harga untuk %s", symbol)
            return None

        # Bersihkan ticker symbol (hapus suffix .JK untuk key)
        clean_symbol = symbol.replace(".JK", "")

        payload = {
            "symbol": clean_symbol,
            "price": round(float(last_price), 2),
            "currency": "IDR",
            "volume": int(volume) if volume is not None else 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "yfinance",
        }

        logger.info(
            " %s — Harga: Rp %s | Volume: %s",
            clean_symbol,
            f"{payload['price']:,.2f}",
            f"{payload['volume']:,}",
        )
        return payload

    except Exception as e:
        logger.error(" Gagal mengambil data %s: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Main Loop: Polling & Publish
# ---------------------------------------------------------------------------
def main():
    # [M. Alfaeran Auriga Ruswandi]: Fungsi utama yang menjalankan loop
    # polling data saham IDX setiap 60 detik dan mengirimkan hasilnya
    # ke Kafka topic 'saham-api'. Setiap pesan menggunakan ticker symbol
    # sebagai Kafka key untuk partisi yang konsisten.
    """
    Entry-point utama. Menjalankan infinite loop yang:
      1. Mengambil data harga dari 5 saham IDX via yfinance.
      2. Mengirim setiap data sebagai pesan JSON ke Kafka topic 'saham-api'.
      3. Menunggu 60 detik sebelum polling berikutnya.
    """

    logger.info("=" * 60)
    logger.info("   Memulai Producer Saham IDX")
    logger.info("   Tickers : %s", ", ".join(TICKERS))
    logger.info("   Topic   : %s", KAFKA_TOPIC)
    logger.info("   Interval: %d detik", POLLING_INTERVAL_SECONDS)
    logger.info("=" * 60)

    # --- Buat Kafka Producer ---
    try:
        producer = create_producer()
    except KafkaError as e:
        logger.critical("Tidak dapat membuat Kafka Producer: %s", e)
        logger.critical("   Pastikan Kafka broker berjalan di %s", KAFKA_BOOTSTRAP_SERVERS)
        return

    # --- Infinite Polling Loop ---
    cycle = 0
    try:
        while True:
            cycle += 1
            logger.info("— Siklus polling #%d —", cycle)

            sent_count = 0
            for symbol in TICKERS:
                data = fetch_stock_data(symbol)
                if data is None:
                    continue

                # Gunakan clean symbol sebagai Kafka key
                kafka_key = data["symbol"]  # e.g. "BBCA"

                try:
                    future = producer.send(
                        topic=KAFKA_TOPIC,
                        key=kafka_key,
                        value=data,
                    )
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    sent_count += 1

                except KafkaError as e:
                    logger.error(
                        "Gagal mengirim data %s ke Kafka: %s",
                        kafka_key,
                        e,
                    )

            # Flush untuk memastikan semua pesan terkirim sebelum tidur
            producer.flush()
            logger.info(
                "Siklus #%d selesai — %d/%d pesan terkirim",
                cycle,
                sent_count,
                len(TICKERS),
            )

            logger.info(
                " Menunggu %d detik sebelum polling berikutnya...\n",
                POLLING_INTERVAL_SECONDS,
            )
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("\nProducer dihentikan oleh pengguna (Ctrl+C)")

    finally:
        producer.flush()
        producer.close()
        logger.info(" Kafka Producer ditutup dengan aman. Selesai.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
