# ============================================================================
# producer_api.py -- Kafka Producer untuk Data Saham IDX (Real-Time Polling)
# ============================================================================
# Skrip ini mengambil harga saham real-time dari Yahoo Finance (yfinance)
# untuk 5 emiten IDX, lalu mengirimkan datanya ke Apache Kafka secara
# periodik setiap 60 detik.
#
# DUAL-WRITE: Data juga disimpan ke file lokal (dashboard/data/live_api.json)
# sehingga dashboard tetap bisa menampilkan data meskipun Docker/Kafka mati.
#
# Topik ETS  : Topik 4 -- Monitor Saham IDX
# Kafka Topic: saham-api
# ============================================================================

import json
import logging
import time
import os
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

# Kafka bersifat opsional -- bisa jalan tanpa Docker
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "saham-api"
TICKERS = ["BBCA.JK", "BBRI.JK", "TLKM.JK", "ASII.JK", "BMRI.JK"]
POLLING_INTERVAL_SECONDS = 60

# Path file lokal untuk dual-write
BASE_DIR = Path(__file__).parent.parent
LIVE_API_FILE = BASE_DIR / "dashboard" / "data" / "live_api.json"

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
            " %s -- Harga: Rp %s | Volume: %s",
            clean_symbol,
            f"{payload['price']:,.2f}",
            f"{payload['volume']:,}",
        )
        return payload

    except Exception as e:
        logger.error(" Gagal mengambil data %s: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Dual-Write: Simpan data ke file lokal (untuk dashboard offline)
# ---------------------------------------------------------------------------
def save_to_local(records: list[dict]):
    """
    Simpan semua record ke live_api.json.
    Data di-append ke file yang sudah ada, sehingga history tetap terjaga.
    Maksimal 2000 record disimpan (FIFO) agar file tidak terlalu besar.
    """
    MAX_RECORDS = 2000
    existing = []
    if LIVE_API_FILE.exists():
        try:
            with open(LIVE_API_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            existing = raw if isinstance(raw, list) else raw.get("records", [])
        except (json.JSONDecodeError, Exception):
            existing = []

    # Append new records
    existing.extend(records)

    # Trim ke MAX_RECORDS terbaru
    if len(existing) > MAX_RECORDS:
        existing = existing[-MAX_RECORDS:]

    # Pastikan directory ada
    LIVE_API_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(LIVE_API_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    logger.info("Disimpan ke lokal: %s (%d records total)", LIVE_API_FILE.name, len(existing))


def run_local_analysis():
    """
    Jalankan analisis lokal dan regenerasi spark_results.json.
    Ini memungkinkan dashboard menampilkan analytics terbaru
    tanpa perlu Spark cluster.
    """
    try:
        analysis_script = BASE_DIR / "spark" / "local_analysis.py"
        if analysis_script.exists():
            import subprocess
            result = subprocess.run(
                ["python", str(analysis_script)],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                logger.info("spark_results.json berhasil di-regenerasi")
            else:
                logger.warning("Gagal regenerasi spark_results.json: %s", result.stderr[:200])
    except Exception as e:
        logger.warning("Skip local analysis: %s", e)


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
      3. Menyimpan data ke file lokal (dual-write).
      4. Menjalankan analisis lokal untuk regenerasi spark_results.json.
      5. Menunggu 60 detik sebelum polling berikutnya.
    """

    logger.info("=" * 60)
    logger.info("   Memulai Producer Saham IDX")
    logger.info("   Tickers : %s", ", ".join(TICKERS))
    logger.info("   Topic   : %s", KAFKA_TOPIC)
    logger.info("   Interval: %d detik", POLLING_INTERVAL_SECONDS)
    logger.info("   File    : %s", LIVE_API_FILE)
    logger.info("=" * 60)

    # --- Buat Kafka Producer (opsional) ---
    producer = None
    kafka_mode = False
    if KAFKA_AVAILABLE:
        try:
            producer = create_producer()
            kafka_mode = True
        except Exception as e:
            logger.warning("Kafka tidak tersedia: %s", e)
            logger.info("Berjalan dalam mode LOCAL-ONLY (tanpa Kafka)")
    else:
        logger.info("kafka-python tidak terinstall, berjalan dalam mode LOCAL-ONLY")

    # --- Infinite Polling Loop ---
    cycle = 0
    try:
        while True:
            cycle += 1
            logger.info("--- Siklus polling #%d ---", cycle)

            sent_count = 0
            cycle_records = []  # kumpulkan data untuk simpan lokal

            for symbol in TICKERS:
                data = fetch_stock_data(symbol)
                if data is None:
                    continue

                cycle_records.append(data)

                # Kirim ke Kafka jika tersedia
                if kafka_mode and producer:
                    kafka_key = data["symbol"]
                    try:
                        future = producer.send(
                            topic=KAFKA_TOPIC,
                            key=kafka_key,
                            value=data,
                        )
                        future.add_callback(on_send_success)
                        future.add_errback(on_send_error)
                        sent_count += 1
                    except Exception as e:
                        logger.error(
                            "Gagal mengirim data %s ke Kafka: %s",
                            kafka_key, e,
                        )

            # Flush Kafka
            if kafka_mode and producer:
                producer.flush()

            # Dual-Write: simpan ke file lokal
            if cycle_records:
                save_to_local(cycle_records)

            # Trigger regenerasi spark_results.json
            run_local_analysis()

            logger.info(
                "Siklus #%d selesai --- %d data diambil, %d ke Kafka, %d ke lokal",
                cycle, len(cycle_records), sent_count, len(cycle_records),
            )

            logger.info(
                "Menunggu %d detik sebelum polling berikutnya...\n",
                POLLING_INTERVAL_SECONDS,
            )
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("\nProducer dihentikan oleh pengguna (Ctrl+C)")

    finally:
        if producer:
            producer.flush()
            producer.close()
        logger.info("[OK] Producer ditutup dengan aman. Selesai.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
