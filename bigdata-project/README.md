# 📊 Panduan Menjalankan Dashboard  
## SahamMeter Dashboard

**Nama:** S. Farhan Baig  
**Posisi:** Anggota 5  
**Tanggung Jawab:** `dashboard/app.py` dan `dashboard/templates/index.html`

Bagian ini menjelaskan cara menjalankan **Dashboard (Serving Layer)** pada project **SahamMeter**. Dashboard bertugas menampilkan hasil analisis data saham, data saham terbaru, dan berita pasar modal dalam bentuk web monitoring yang dapat diakses melalui browser.

### 1. Install dependency (jika belum)

```bash
pip install flask pandas
```

**Kegunaan:**  

- **Flask** → menjalankan web server dashboard.  
- **Pandas** → membantu membaca dan mengolah data JSON hasil pipeline.

Langkah ini cukup dilakukan sekali.

---

### 2. Jalankan dashboard

```bash
python3 dashboard/app.py
```

**Kegunaan:**  
Menjalankan aplikasi dashboard SahamMeter menggunakan Flask di local server.

Jika berhasil, dashboard akan berjalan di port 5000.

---

### 3. Buka dashboard di browser

```txt
http://localhost:5000
```

**Kegunaan:**  
Menampilkan front-end dashboard yang berisi:

- hasil analisis Spark
- live stock data
- berita terbaru

Dashboard akan melakukan refresh data secara otomatis.