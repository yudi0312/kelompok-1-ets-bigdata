# **ETS Praktik: Big Data Pipeline End-to-End**
---
## **MATA KULIAH BIG DATA & DATA LAKEHOUSE 2026**

| Nama                          | NRP        |
|-------------------------------|------------|
| Putu Yudi Nandanjaya Wiraguna | 5027241080 |
| S. Farhan Baig  | 5027241097 |
| Christiano Ronaldo Silalahi | 5027241025 |
| M. Alfaeran Auriga Ruswandi | 5027241115 | 
| Fika Arka Nuriyah  | 5027241071 |


## **Topik yang Dipilih**
**SahamMeter** adalah sistem monitoring yang menggabungkan **data harga saham real-time** dengan **berita pasar modal** untuk memberikan insight yang lebih lengkap terhadap pergerakan saham.

## **Latar Belakang**
Dalam dunia investasi, harga saham tidak hanya dipengaruhi oleh angka, tetapi juga oleh:
- Sentimen berita  
- Kondisi ekonomi  
- Performa perusahaan  

Oleh karena itu, analisis yang hanya berbasis harga **tidak cukup**.


## **Mengapa Topik Ini Menarik?
**
### ** 1. Integrasi Data Real-Time & Berita**
Menggabungkan:
- API saham (Yahoo Finance)
- RSS berita pasar modal  

Memberikan **analisis kontekstual**, bukan hanya angka.


### **2. Insight Lebih Mendalam**
Sistem dapat menjawab pertanyaan penting:

*“Saham blue-chip mana yang paling aktif hari ini, dan berita apa yang mempengaruhinya?”*



### **3. Relevansi Dunia Nyata**
Digunakan oleh:
- Wealth Management Company  
- Analis Saham  
- Investor  

Untuk:
- Monitoring pasar harian  
- Laporan ke nasabah  
- Pengambilan keputusan investasi  

---

### **4. Implementasi Big Data Pipeline**
Menggunakan arsitektur modern:
- Kafka → ingestion data  
- HDFS → storage  
- Spark → analisis  
- Dashboard → visualisasi  

Sistem menjadi **scalable dan real-time**

<img width="1536" height="1024" alt="ChatGPT Image May 5, 2026, 08_09_19 AM" src="https://github.com/user-attachments/assets/7bdc5da4-9d96-4c7d-b43f-13ed5e413e8c" />

# Panduan Menjalankan Dashboard  
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
