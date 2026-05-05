# ============================================================================
# local_analysis.py — Analisis Data Saham (HDFS -> spark_results.json -> HDFS)
# ============================================================================
# Skrip ini membaca data dari HDFS yang sudah disimpan oleh consumer_to_hdfs.py,
# menghitung analisis return, volatilitas, dan frekuensi penyebutan,
# lalu menyimpan hasilnya ke dashboard/data/spark_results.json.
#
# Alur data (pipeline utama):
#   Producer -> Kafka -> Consumer -> HDFS -> [skrip ini] -> spark_results.json
#
# Jika HDFS tidak tersedia (Docker mati), skrip ini akan fallback ke
# file lokal (dashboard/data/live_api.json dan live_rss.json) sebagai
# sumber data cadangan.
#
# Bisa dijalankan manual:
#   python spark/local_analysis.py
#
# Atau dipanggil otomatis oleh producer_api.py setelah setiap siklus polling.
# ============================================================================

import json
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "dashboard" / "data"
LIVE_API_FILE = DATA_DIR / "live_api.json"
LIVE_RSS_FILE = DATA_DIR / "live_rss.json"
SPARK_RESULTS_FILE = DATA_DIR / "spark_results.json"

# HDFS Configuration (sama dengan consumer_to_hdfs.py)
HDFS_URL = "http://localhost:9870"
HDFS_USER = "hadoop"  # Eksplisit agar tidak pakai username Windows (spasi)
HDFS_PATH_API = "/data/saham/api/"
HDFS_PATH_RSS = "/data/saham/rss/"
HDFS_PATH_RESULTS = "/data/saham/hasil_analisis/"
# Map container hostnames -> localhost (untuk WebHDFS redirect ke DataNode)
HDFS_HOST_ALIASES = {"datanode": "localhost", "namenode": "localhost"}


def _create_hdfs_client():
    """Buat InsecureClient dengan resolver khusus agar hostname Docker
    (datanode, namenode) di-resolve ke localhost dari mesin host Windows."""
    from hdfs import InsecureClient
    import requests

    class _AliasedSession(requests.Session):
        """Session yang me-rewrite hostname container -> localhost."""
        def send(self, request, **kwargs):
            from urllib.parse import urlparse, urlunparse
            parsed = urlparse(request.url)
            alias = HDFS_HOST_ALIASES.get(parsed.hostname)
            if alias:
                new = parsed._replace(
                    netloc=f"{alias}:{parsed.port}" if parsed.port else alias
                )
                request.url = urlunparse(new)
                # Tetap kirim Host header asli agar WebHDFS tidak bingung
                request.headers["Host"] = (
                    f"{parsed.hostname}:{parsed.port}" if parsed.port else parsed.hostname
                )
            return super().send(request, **kwargs)

    session = _AliasedSession()
    return InsecureClient(HDFS_URL, root="/", user=HDFS_USER, session=session)

TICKER_MAP = {
    "BBCA": {"nama": "Bank Central Asia",     "keyword": "BCA",     "sektor": "Perbankan"},
    "BBRI": {"nama": "Bank Rakyat Indonesia", "keyword": "BRI",     "sektor": "Perbankan"},
    "TLKM": {"nama": "Telkom Indonesia",      "keyword": "Telkom",  "sektor": "Telekomunikasi"},
    "ASII": {"nama": "Astra International",   "keyword": "Astra",   "sektor": "Otomotif"},
    "BMRI": {"nama": "Bank Mandiri",          "keyword": "Mandiri", "sektor": "Perbankan"},
}


# ---------------------------------------------------------------------------
# Loader: Baca data dari HDFS (sumber utama)
# ---------------------------------------------------------------------------
def load_from_hdfs_api() -> list[dict]:
    """
    Baca semua file JSON dari HDFS path /data/saham/api/.
    Setiap file berisi satu record harga saham yang ditulis oleh consumer_to_hdfs.py.
    """
    try:
        client = _create_hdfs_client()

        records = []
        # List semua file di HDFS path
        files = client.list(HDFS_PATH_API)
        logger.info("HDFS %s: ditemukan %d file", HDFS_PATH_API, len(files))

        for filename in files:
            if not filename.endswith(".json"):
                continue
            filepath = HDFS_PATH_API + filename
            try:
                with client.read(filepath, encoding="utf-8") as reader:
                    data = json.load(reader)
                records.append(data)
            except Exception as e:
                logger.warning("Gagal baca %s: %s", filepath, e)

        logger.info("[OK] HDFS API: %d records berhasil dibaca", len(records))
        return records

    except ImportError:
        logger.warning("hdfs library tidak terinstall, skip HDFS")
        return []
    except Exception as e:
        logger.warning("HDFS tidak tersedia: %s", e)
        return []


def load_from_hdfs_rss() -> list[dict]:
    """
    Baca semua file JSON dari HDFS path /data/saham/rss/.
    Setiap file berisi satu artikel berita yang ditulis oleh consumer_to_hdfs.py.
    """
    try:
        client = _create_hdfs_client()

        articles = []
        files = client.list(HDFS_PATH_RSS)
        logger.info("HDFS %s: ditemukan %d file", HDFS_PATH_RSS, len(files))

        for filename in files:
            if not filename.endswith(".json"):
                continue
            filepath = HDFS_PATH_RSS + filename
            try:
                with client.read(filepath, encoding="utf-8") as reader:
                    data = json.load(reader)
                articles.append(data)
            except Exception as e:
                logger.warning("Gagal baca %s: %s", filepath, e)

        logger.info("[OK] HDFS RSS: %d articles berhasil dibaca", len(articles))
        return articles

    except ImportError:
        logger.warning("hdfs library tidak terinstall, skip HDFS")
        return []
    except Exception as e:
        logger.warning("HDFS tidak tersedia: %s", e)
        return []


# ---------------------------------------------------------------------------
# Loader: Fallback ke file lokal (jika HDFS tidak tersedia)
# ---------------------------------------------------------------------------
def load_local_api() -> list[dict]:
    """Fallback: baca data harga saham dari live_api.json lokal."""
    if not LIVE_API_FILE.exists():
        return []
    try:
        with open(LIVE_API_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        records = raw if isinstance(raw, list) else raw.get("records", [])
        logger.info("Lokal fallback: %d records dari live_api.json", len(records))
        return records
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Gagal membaca live_api.json: %s", e)
        return []


def load_local_rss() -> list[dict]:
    """Fallback: baca data berita dari live_rss.json lokal."""
    if not LIVE_RSS_FILE.exists():
        return []
    try:
        with open(LIVE_RSS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        articles = raw if isinstance(raw, list) else raw.get("articles", [])
        logger.info("Lokal fallback: %d articles dari live_rss.json", len(articles))
        return articles
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("Gagal membaca live_rss.json: %s", e)
        return []


# ---------------------------------------------------------------------------
# Loader: Coba HDFS dulu, fallback ke lokal
# ---------------------------------------------------------------------------
def load_api_data() -> tuple[list[dict], str]:
    """
    Baca data harga saham. Prioritas:
      1. HDFS /data/saham/api/  (production — dari consumer Kafka)
      2. Lokal live_api.json    (fallback — dari dual-write producer)

    Returns:
        (records, sumber) — list of records dan string sumber data
    """
    # Coba HDFS dulu
    records = load_from_hdfs_api()
    if records:
        return records, "HDFS"

    # Fallback ke lokal
    records = load_local_api()
    if records:
        return records, "lokal (live_api.json)"

    return [], "tidak ada"


def load_rss_data() -> tuple[list[dict], str]:
    """
    Baca data berita. Prioritas:
      1. HDFS /data/saham/rss/  (production — dari consumer Kafka)
      2. Lokal live_rss.json    (fallback — dari dual-write producer)

    Returns:
        (articles, sumber) — list of articles dan string sumber data
    """
    # Coba HDFS dulu
    articles = load_from_hdfs_rss()
    if articles:
        return articles, "HDFS"

    # Fallback ke lokal
    articles = load_local_rss()
    if articles:
        return articles, "lokal (live_rss.json)"

    return [], "tidak ada"


# ---------------------------------------------------------------------------
# Analisis Return Saham
# ---------------------------------------------------------------------------
def analyze_return(records: list[dict]) -> list[dict]:
    """
    Hitung return (%) untuk setiap saham berdasarkan harga awal dan akhir.
    Logika sama dengan analysis.ipynb di Spark.
    """
    grouped = defaultdict(list)
    for r in records:
        sym = r.get("symbol", r.get("ticker", "")).replace(".JK", "")
        price = r.get("close", r.get("price", r.get("harga", 0)))
        ts = r.get("timestamp", r.get("waktu", ""))
        if sym and price:
            grouped[sym].append({"ts": str(ts), "close": float(price)})

    results = []
    for sym, items in grouped.items():
        items.sort(key=lambda x: x["ts"])
        h_awal = items[0]["close"]
        h_akhir = items[-1]["close"]
        ret = (h_akhir - h_awal) / h_awal * 100 if h_awal else 0

        info = TICKER_MAP.get(sym, {})
        results.append({
            "symbol": sym,
            "nama_perusahaan": info.get("nama", sym),
            "sektor": info.get("sektor", "-"),
            "harga_awal": h_awal,
            "harga_akhir": h_akhir,
            "return_pct": round(ret, 2),
            "selisih_rp": round(h_akhir - h_awal, 0),
            "waktu_awal": items[0]["ts"],
            "waktu_akhir": items[-1]["ts"],
        })

    return sorted(results, key=lambda x: abs(x["return_pct"]), reverse=True)


# ---------------------------------------------------------------------------
# Analisis Volatilitas
# ---------------------------------------------------------------------------
def analyze_volatilitas(records: list[dict]) -> list[dict]:
    """
    Hitung statistik volatilitas (std dev, CV, range) untuk setiap saham.
    """
    grouped = defaultdict(list)
    for r in records:
        sym = r.get("symbol", r.get("ticker", "")).replace(".JK", "")
        price = r.get("close", r.get("price", r.get("harga", 0)))
        if sym and price:
            grouped[sym].append(float(price))

    results = []
    for sym, prices in grouped.items():
        if not prices:
            continue
        mean = sum(prices) / len(prices)
        variance = sum((x - mean) ** 2 for x in prices) / len(prices)
        std = variance ** 0.5
        cv = (std / mean * 100) if mean else 0

        info = TICKER_MAP.get(sym, {})
        results.append({
            "symbol": sym,
            "nama_perusahaan": info.get("nama", sym),
            "sektor": info.get("sektor", "-"),
            "jumlah_data_poin": len(prices),
            "harga_rata_rata": round(mean, 0),
            "harga_terendah": round(min(prices), 0),
            "harga_tertinggi": round(max(prices), 0),
            "range_harga": round(max(prices) - min(prices), 0),
            "stddev_harga": round(std, 2),
            "cv_pct": round(cv, 4),
        })

    return sorted(results, key=lambda x: x["cv_pct"], reverse=True)


# ---------------------------------------------------------------------------
# Analisis Frekuensi Penyebutan di Berita
# ---------------------------------------------------------------------------
def analyze_frekuensi(articles: list[dict]) -> list[dict]:
    """
    Hitung berapa kali setiap keyword saham disebutkan di berita.
    """
    results = []
    for sym, info in TICKER_MAP.items():
        kw = info["keyword"].lower()
        sebutan = 0
        for a in articles:
            # Support format dari HDFS (title/summary) dan lokal (judul/ringkasan)
            text = (
                a.get("judul", a.get("title", "")) + " " +
                a.get("ringkasan", a.get("summary", ""))
            ).lower()
            if kw in text:
                sebutan += 1

        results.append({
            "keyword": info["keyword"],
            "perusahaan": info["nama"],
            "symbol": sym,
            "nama_perusahaan": info["nama"],
            "sektor": info["sektor"],
            "jumlah_sebutan": sebutan,
        })

    return sorted(results, key=lambda x: x["jumlah_sebutan"], reverse=True)


# ---------------------------------------------------------------------------
# Simpan Hasil Analisis ke HDFS
# ---------------------------------------------------------------------------
def save_results_to_hdfs(spark_results: dict) -> str | None:
    """
    Simpan hasil analisis (spark_results) ke HDFS di /data/saham/hasil_analisis/.
    File disimpan dengan nama bertimestamp + file latest.json yang selalu di-overwrite.

    Returns:
        Path HDFS yang berhasil ditulis, atau None jika gagal.
    """
    try:
        client = _create_hdfs_client()

        # Pastikan direktori HDFS ada
        client.makedirs(HDFS_PATH_RESULTS)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename_ts = f"spark_results_{timestamp}.json"
        filename_latest = "spark_results_latest.json"

        payload = json.dumps(spark_results, ensure_ascii=False, indent=2)

        # 1. Simpan file bertimestamp (histori)
        path_ts = HDFS_PATH_RESULTS + filename_ts
        with client.write(path_ts, encoding="utf-8") as writer:
            writer.write(payload)
        logger.info("[OK] Hasil analisis disimpan ke HDFS: %s", path_ts)

        # 2. Simpan/overwrite latest.json (selalu yang terbaru)
        path_latest = HDFS_PATH_RESULTS + filename_latest
        with client.write(path_latest, encoding="utf-8", overwrite=True) as writer:
            writer.write(payload)
        logger.info("[OK] HDFS latest diperbarui: %s", path_latest)

        return path_ts

    except ImportError:
        logger.warning("hdfs library tidak terinstall, skip simpan ke HDFS")
        return None
    except Exception as e:
        logger.warning("Gagal menyimpan hasil analisis ke HDFS: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main: Jalankan semua analisis dan simpan ke spark_results.json
# ---------------------------------------------------------------------------
def main():
    now = datetime.now()
    logger.info("=" * 60)
    logger.info("   Analisis Data Saham - HDFS -> spark_results.json")
    logger.info("   Waktu: %s", now.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # Load data (HDFS dulu, fallback ke lokal)
    api_records, sumber_api = load_api_data()
    rss_articles, sumber_rss = load_rss_data()

    logger.info("Sumber data API : %s (%d records)", sumber_api, len(api_records))
    logger.info("Sumber data RSS : %s (%d articles)", sumber_rss, len(rss_articles))

    if not api_records and not rss_articles:
        logger.warning("Tidak ada data untuk dianalisis. Skip.")
        return

    # Jalankan analisis
    analisis_return = analyze_return(api_records) if api_records else []
    analisis_volatilitas = analyze_volatilitas(api_records) if api_records else []
    analisis_frekuensi = analyze_frekuensi(rss_articles)

    # Tentukan label sumber
    if sumber_api == "HDFS" or sumber_rss == "HDFS":
        label_sumber = "HDFS -> local_analysis.py"
    else:
        label_sumber = "lokal -> local_analysis.py (fallback, HDFS tidak tersedia)"

    # Susun hasil akhir (format sama dengan output Spark di analysis.ipynb)
    spark_results = {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "is_market_open": 9 <= now.hour < 16,
        "total_records_api": len(api_records),
        "total_articles_rss": len(rss_articles),
        "analisis_return": analisis_return,
        "analisis_volatilitas": analisis_volatilitas,
        "analisis_frekuensi": analisis_frekuensi,
        "berita": rss_articles[:10],
        "_sumber": label_sumber,
        "_sumber_api": sumber_api,
        "_sumber_rss": sumber_rss,
    }

    # Simpan ke spark_results.json (lokal)
    SPARK_RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SPARK_RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(spark_results, f, ensure_ascii=False, indent=2)

    logger.info("[OK] spark_results.json berhasil ditulis (lokal)")

    # Simpan juga ke HDFS
    hdfs_saved = save_results_to_hdfs(spark_results)

    logger.info("=" * 60)
    logger.info("Ringkasan Analisis")
    logger.info("   Sumber  : %s", label_sumber)
    logger.info("   Return  : %d saham dianalisis", len(analisis_return))
    logger.info("   Vola    : %d saham dianalisis", len(analisis_volatilitas))
    logger.info("   Frekuensi: %d keyword dihitung", len(analisis_frekuensi))
    logger.info("   Berita  : %d artikel tersedia", len(rss_articles))
    logger.info("   Lokal   : %s", SPARK_RESULTS_FILE)
    logger.info("   HDFS    : %s", hdfs_saved if hdfs_saved else "gagal / tidak tersedia")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
