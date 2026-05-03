# Flask server yang menyajikan data saham IDX blue-chip dan berita pasar modal.
#
# Sumber data (dengan fallback):
#   1. HDFS /data/saham/api/ dan /data/saham/rss/  (production)
#   2. Kafka topic saham-api dan saham-rss          (live streaming)
#   3. dashboard/data/spark_results.json            (hasil analisis Spark)
#   4. Yahoo Finance via yfinance                   (fallback live)
#   5. Data dummy                                   (fallback terakhir)
# =============================================================================

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, render_template, jsonify

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")

# ---------------------------------------------------------------------------
# Konfigurasi Path & Konstanta
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SPARK_RESULTS_FILE = DATA_DIR / "spark_results.json"
LIVE_API_FILE = DATA_DIR / "live_api.json"
LIVE_RSS_FILE = DATA_DIR / "live_rss.json"

TICKERS = ["BBCA", "BBRI", "TLKM", "ASII", "BMRI"]
TICKER_MAP = {
    "BBCA": {"nama": "Bank Central Asia",    "keyword": "BCA",    "sektor": "Perbankan"},
    "BBRI": {"nama": "Bank Rakyat Indonesia","keyword": "BRI",    "sektor": "Perbankan"},
    "TLKM": {"nama": "Telkom Indonesia",     "keyword": "Telkom", "sektor": "Telekomunikasi"},
    "ASII": {"nama": "Astra International",  "keyword": "Astra",  "sektor": "Otomotif"},
    "BMRI": {"nama": "Bank Mandiri",         "keyword": "Mandiri","sektor": "Perbankan"},
}

# ---------------------------------------------------------------------------
# Loader: Spark Results (output dari analisis Spark — sumber utama)
# ---------------------------------------------------------------------------
def load_spark_results() -> dict | None:
    """Baca hasil analisis Spark dari file JSON lokal."""
    if SPARK_RESULTS_FILE.exists():
        try:
            with open(SPARK_RESULTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info("Loaded spark_results.json (%d return records)", len(data.get("analisis_return", [])))
            return data
        except Exception as e:
            logger.warning("Gagal membaca spark_results.json: %s", e)
    return None


# ---------------------------------------------------------------------------
# Loader: Live API JSON (output consumer Kafka → file lokal)
# ---------------------------------------------------------------------------
def load_live_api() -> list[dict]:
    """Baca data harga saham live dari file live_api.json."""
    if LIVE_API_FILE.exists():
        try:
            with open(LIVE_API_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            # raw bisa berupa list of records atau dict dengan key "records"
            records = raw if isinstance(raw, list) else raw.get("records", [])
            logger.info("Loaded live_api.json (%d records)", len(records))
            return records
        except Exception as e:
            logger.warning("Gagal membaca live_api.json: %s", e)
    return []


# ---------------------------------------------------------------------------
# Loader: Live RSS JSON (output consumer Kafka → file lokal)
# ---------------------------------------------------------------------------
def load_live_rss() -> list[dict]:
    """Baca data berita live dari file live_rss.json."""
    if LIVE_RSS_FILE.exists():
        try:
            with open(LIVE_RSS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            articles = raw if isinstance(raw, list) else raw.get("articles", [])
            logger.info("Loaded live_rss.json (%d articles)", len(articles))
            return articles
        except Exception as e:
            logger.warning("Gagal membaca live_rss.json: %s", e)
    return []


# ---------------------------------------------------------------------------
# Loader: Yahoo Finance fallback (jika tidak ada data lokal)
# ---------------------------------------------------------------------------
def fetch_from_yfinance() -> list[dict]:
    """Ambil data harga saham langsung dari Yahoo Finance sebagai fallback."""
    try:
        import yfinance as yf
        records = []
        for ticker in TICKERS:
            symbol = f"{ticker}.JK"
            try:
                t = yf.Ticker(symbol)
                hist = t.history(period="1d", interval="1m")
                if hist.empty:
                    continue
                for ts, row in hist.iterrows():
                    records.append({
                        "symbol": ticker,
                        "timestamp": str(ts),
                        "open":  float(row["Open"]),
                        "high":  float(row["High"]),
                        "low":   float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row["Volume"]),
                    })
            except Exception as e:
                logger.warning("yfinance error untuk %s: %s", ticker, e)
        logger.info("yfinance fallback: %d records", len(records))
        return records
    except ImportError:
        logger.warning("yfinance tidak terinstall, skip fallback")
        return []


# ---------------------------------------------------------------------------
# Generator: Data Dummy (fallback terakhir agar dashboard tetap bisa jalan)
# ---------------------------------------------------------------------------
def generate_dummy_data() -> dict:
    """
    Hasilkan data saham dummy yang realistis agar dashboard bisa didemonstrasikan
    tanpa koneksi ke Kafka/HDFS/Yahoo Finance.
    """
    import random
    random.seed(42)

    harga_base = {"BBCA": 9500, "BBRI": 4200, "TLKM": 3800, "ASII": 5800, "BMRI": 6100}
    now = datetime.now()

    # Simulasi intraday price series (75 titik per saham, interval 1 menit)
    semua_records = []
    for ticker, base in harga_base.items():
        harga = base
        for i in range(75):
            ts = now - timedelta(minutes=(74 - i))
            perubahan = random.gauss(0, base * 0.002)
            harga = max(harga + perubahan, base * 0.90)
            semua_records.append({
                "symbol": ticker,
                "timestamp": ts.isoformat(),
                "close": round(harga, 0),
                "open":  round(harga - random.uniform(-50, 50), 0),
                "high":  round(harga + abs(random.gauss(0, 30)), 0),
                "low":   round(harga - abs(random.gauss(0, 30)), 0),
                "volume": random.randint(1_000_000, 50_000_000),
            })

    # Hitung analisis dari records
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in semua_records:
        grouped[r["symbol"]].append(r["close"])

    analisis_return = []
    analisis_volatilitas = []
    for ticker, harga_list in grouped.items():
        h_awal = harga_list[0]
        h_akhir = harga_list[-1]
        ret = (h_akhir - h_awal) / h_awal * 100 if h_awal else 0

        mean = sum(harga_list) / len(harga_list)
        variance = sum((x - mean) ** 2 for x in harga_list) / len(harga_list)
        std = variance ** 0.5
        cv = (std / mean * 100) if mean else 0

        analisis_return.append({
            "symbol": ticker,
            "harga_awal": h_awal,
            "harga_akhir": h_akhir,
            "return_pct": round(ret, 2),
            "selisih_rp": round(h_akhir - h_awal, 0),
            "waktu_awal": (now - timedelta(minutes=74)).strftime("%Y-%m-%d %H:%M:%S"),
            "waktu_akhir": now.strftime("%Y-%m-%d %H:%M:%S"),
        })
        analisis_volatilitas.append({
            "symbol": ticker,
            "jumlah_data_poin": len(harga_list),
            "harga_rata_rata": round(mean, 0),
            "harga_terendah": round(min(harga_list), 0),
            "harga_tertinggi": round(max(harga_list), 0),
            "range_harga": round(max(harga_list) - min(harga_list), 0),
            "stddev_harga": round(std, 2),
            "cv_pct": round(cv, 4),
        })

    # Dummy berita
    berita_dummy = [
        {
            "judul": "BCA Cetak Laba Bersih Rp 12,4 Triliun di Kuartal I 2026",
            "sumber": "Bisnis.com",
            "url": "https://bisnis.com",
            "waktu_publikasi": (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M"),
            "keyword_terdeteksi": ["BCA", "BBCA"],
            "ringkasan": "Bank Central Asia (BCA) membukukan laba bersih senilai Rp 12,4 triliun pada kuartal pertama 2026, tumbuh 14% secara year-on-year.",
        },
        {
            "judul": "Telkom Indonesia Ekspansi Layanan 5G ke 10 Kota Baru",
            "sumber": "Kontan.co.id",
            "url": "https://kontan.co.id",
            "waktu_publikasi": (now - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M"),
            "keyword_terdeteksi": ["Telkom", "TLKM"],
            "ringkasan": "TLKM mengumumkan rencana ekspansi jaringan 5G ke 10 kota tier-2, yang diproyeksikan meningkatkan pendapatan layanan data sebesar 20%.",
        },
        {
            "judul": "Astra International Catat Penjualan Kendaraan Naik 8% YoY",
            "sumber": "CNBC Indonesia",
            "url": "https://cnbcindonesia.com",
            "waktu_publikasi": (now - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M"),
            "keyword_terdeteksi": ["Astra", "ASII"],
            "ringkasan": "Penjualan kendaraan Astra International mencapai 158.000 unit di Q1 2026, naik 8% dari periode yang sama tahun lalu.",
        },
        {
            "judul": "Bank Mandiri dan BRI Kompak Turunkan Suku Bunga KPR",
            "sumber": "Detik Finance",
            "url": "https://detik.com/finance",
            "waktu_publikasi": (now - timedelta(hours=8)).strftime("%Y-%m-%d %H:%M"),
            "keyword_terdeteksi": ["Mandiri", "BMRI", "BRI", "BBRI"],
            "ringkasan": "Bank Mandiri dan BRI secara bersamaan menurunkan suku bunga KPR 25 bps sebagai respons kebijakan BI yang dovish.",
        },
    ]

    frekuensi = []
    for ticker, info in TICKER_MAP.items():
        sebutan = sum(
            1 for b in berita_dummy if info["keyword"] in " ".join(b.get("keyword_terdeteksi", []))
        )
        frekuensi.append({
            "keyword": info["keyword"],
            "perusahaan": info["nama"],
            "symbol": ticker,
            "jumlah_sebutan": sebutan,
        })

    return {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "is_market_open": 9 <= now.hour < 16,
        "total_records_api": len(semua_records),
        "total_articles_rss": len(berita_dummy),
        "analisis_return": sorted(analisis_return, key=lambda x: abs(x["return_pct"]), reverse=True),
        "analisis_volatilitas": sorted(analisis_volatilitas, key=lambda x: x["cv_pct"], reverse=True),
        "analisis_frekuensi": sorted(frekuensi, key=lambda x: x["jumlah_sebutan"], reverse=True),
        "berita": berita_dummy,
        "_sumber": "dummy",
    }


# ---------------------------------------------------------------------------
# Builder: Rakit payload dashboard dari berbagai sumber data
# ---------------------------------------------------------------------------
def build_dashboard_data() -> dict:
    """
    Prioritas sumber data:
    1. spark_results.json  → analisis siap pakai dari Spark
    2. live_api.json       → rekonstruksi analisis dari data mentah Kafka
    3. yfinance            → fallback live langsung dari Yahoo Finance
    4. data dummy          → fallback terakhir agar dashboard tetap jalan
    """
    # --- Coba Spark results ---
    spark = load_spark_results()
    live_records = load_live_api()
    live_rss     = load_live_rss()

    if spark:
        # Gunakan analisis Spark, tapi tambahkan berita dari live_rss jika ada
        if live_rss:
            spark["berita"] = live_rss[:10]
        elif "berita" not in spark:
            # Buat dummy berita jika tidak ada
            dummy = generate_dummy_data()
            spark["berita"] = dummy["berita"]
        spark.setdefault("_sumber", "spark_results.json")
        _enrich_ticker_info(spark)
        return spark

    # --- Coba rekonstruksi dari live_api ---
    if not live_records:
        live_records = fetch_from_yfinance()

    if live_records:
        data = _compute_analytics(live_records, live_rss)
        data.setdefault("_sumber", "live_api / yfinance")
        return data

    # --- Fallback dummy ---
    logger.warning("Menggunakan data dummy (tidak ada sumber data tersedia)")
    return generate_dummy_data()


def _enrich_ticker_info(data: dict):
    """Tambahkan info nama & sektor perusahaan ke setiap record."""
    for key in ("analisis_return", "analisis_volatilitas", "analisis_frekuensi"):
        for item in data.get(key, []):
            sym = item.get("symbol", "")
            if sym in TICKER_MAP:
                item["nama_perusahaan"] = TICKER_MAP[sym]["nama"]
                item["sektor"] = TICKER_MAP[sym]["sektor"]


def _compute_analytics(records: list[dict], rss_articles: list[dict]) -> dict:
    """Hitung return, volatilitas, dan frekuensi dari data mentah."""
    from collections import defaultdict
    grouped = defaultdict(list)
    for r in records:
        sym = r.get("symbol", r.get("ticker", "")).replace(".JK", "")
        price = r.get("close", r.get("price", r.get("harga", 0)))
        ts = r.get("timestamp", r.get("waktu", ""))
        if sym and price:
            grouped[sym].append({"ts": ts, "close": float(price)})

    analisis_return = []
    analisis_volatilitas = []
    now = datetime.now()

    for sym, items in grouped.items():
        items.sort(key=lambda x: x["ts"])
        harga_list = [i["close"] for i in items]
        h_awal, h_akhir = harga_list[0], harga_list[-1]
        ret = (h_akhir - h_awal) / h_awal * 100 if h_awal else 0
        mean = sum(harga_list) / len(harga_list)
        variance = sum((x - mean) ** 2 for x in harga_list) / len(harga_list)
        std = variance ** 0.5
        cv = (std / mean * 100) if mean else 0

        info = TICKER_MAP.get(sym, {})
        analisis_return.append({
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
        analisis_volatilitas.append({
            "symbol": sym,
            "nama_perusahaan": info.get("nama", sym),
            "sektor": info.get("sektor", "-"),
            "jumlah_data_poin": len(harga_list),
            "harga_rata_rata": round(mean, 0),
            "harga_terendah": round(min(harga_list), 0),
            "harga_tertinggi": round(max(harga_list), 0),
            "range_harga": round(max(harga_list) - min(harga_list), 0),
            "stddev_harga": round(std, 2),
            "cv_pct": round(cv, 4),
        })

    # Frekuensi penyebutan di berita
    analisis_frekuensi = []
    for sym, info in TICKER_MAP.items():
        kw = info["keyword"].lower()
        sebutan = sum(
            1 for a in rss_articles
            if kw in (a.get("judul", "") + " " + a.get("ringkasan", "")).lower()
        )
        analisis_frekuensi.append({
            "keyword": info["keyword"],
            "perusahaan": info["nama"],
            "symbol": sym,
            "nama_perusahaan": info["nama"],
            "sektor": info["sektor"],
            "jumlah_sebutan": sebutan,
        })

    return {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "is_market_open": 9 <= now.hour < 16,
        "total_records_api": len(records),
        "total_articles_rss": len(rss_articles),
        "analisis_return": sorted(analisis_return, key=lambda x: abs(x["return_pct"]), reverse=True),
        "analisis_volatilitas": sorted(analisis_volatilitas, key=lambda x: x["cv_pct"], reverse=True),
        "analisis_frekuensi": sorted(analisis_frekuensi, key=lambda x: x["jumlah_sebutan"], reverse=True),
        "berita": rss_articles[:10],
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Render halaman utama dashboard."""
    return render_template("index.html")


@app.route("/api/dashboard")
def api_dashboard():
    """
    API endpoint JSON untuk data dashboard lengkap.
    Dipanggil oleh JavaScript di index.html setiap 60 detik.
    """
    try:
        data = build_dashboard_data()
        _enrich_ticker_info(data)
        return jsonify({"status": "ok", "data": data})
    except Exception as e:
        logger.error("Error building dashboard data: %s", e, exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/saham")
def api_saham():
    """API endpoint: hanya data return & volatilitas saham."""
    data = build_dashboard_data()
    _enrich_ticker_info(data)
    return jsonify({
        "analisis_return": data.get("analisis_return", []),
        "analisis_volatilitas": data.get("analisis_volatilitas", []),
        "generated_at": data.get("generated_at"),
        "is_market_open": data.get("is_market_open"),
    })


@app.route("/api/berita")
def api_berita():
    """API endpoint: hanya data berita dan frekuensi penyebutan."""
    data = build_dashboard_data()
    return jsonify({
        "berita": data.get("berita", []),
        "analisis_frekuensi": data.get("analisis_frekuensi", []),
        "total_articles_rss": data.get("total_articles_rss", 0),
    })


@app.route("/health")
def health():
    """Health check endpoint."""
    spark_ok = SPARK_RESULTS_FILE.exists()
    live_api_ok = LIVE_API_FILE.exists()
    live_rss_ok = LIVE_RSS_FILE.exists()
    return jsonify({
        "status": "ok",
        "spark_results": "ada" if spark_ok else "tidak ada",
        "live_api": "ada" if live_api_ok else "tidak ada",
        "live_rss": "ada" if live_rss_ok else "tidak ada",
        "timestamp": datetime.now().isoformat(),
    })


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("SahamMeter Dashboard — Anggota 5")
    logger.info("Akses: http://localhost:5000")
    logger.info("=" * 60)
    app.run(host="0.0.0.0", port=5000, debug=True)
