import feedparser
import hashlib
import json
import time
from kafka import KafkaProducer

# Ganti topik Kafka menjadi saham-rss untuk berita pasar modal
KAFKA_TOPIC = "saham-rss"  # Topik untuk berita saham IDX
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
RSS_FEEDS = [
    "https://rss.bisnis.com/feed/rss2/financial-market",  # RSS Feed untuk berita pasar modal
    "https://www.cnnindonesia.com/ekonomi/rss"  # Backup feed untuk berita pasar modal
]

def get_hash(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()[:8]

def send_to_kafka(producer, data):
    producer.send(KAFKA_TOPIC, key=data["hash"], value=data)
    print(f"Artikel terkirim: {data['title']}")

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8')
    )

    sent_hashes = set()  # hindari duplikat

    while True:
        for rss_url in RSS_FEEDS:
            feed = feedparser.parse(rss_url)
            print(f"Mengambil {len(feed.entries)} artikel dari {rss_url}")
            for entry in feed.entries:
                h = get_hash(entry.link)
                if h in sent_hashes:
                    continue  # skip duplikat
                data = {
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "summary": entry.get("summary", ""),
                    "published": entry.get("published", ""),
                    "hash": h,
                }
                send_to_kafka(producer, data)
                sent_hashes.add(h)
        producer.flush()
        print("Menunggu 5 menit...")
        time.sleep(300)

if __name__ == "__main__":
    main()