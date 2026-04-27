import json
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient

KAFKA_TOPIC_API = "crypto-api"
KAFKA_TOPIC_RSS = "crypto-rss"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_PATH_API = "/data/crypto/api/"
HDFS_PATH_RSS = "/data/crypto/rss/"

# ✅ Fix: tambah value_deserializer
consumer = KafkaConsumer(
    KAFKA_TOPIC_API,
    KAFKA_TOPIC_RSS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="consumer-group",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ✅ Fix: port 9870 sesuai docker-compose
hdfs_client = InsecureClient("http://localhost:9870", root="/", timeout=10)

def save_to_hdfs(topic, data):
    hdfs_path = HDFS_PATH_API if topic == KAFKA_TOPIC_API else HDFS_PATH_RSS
    import uuid
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S") + "_" + str(uuid.uuid4())[:8]
    filename = f"{timestamp}.json"
    with hdfs_client.write(hdfs_path + filename, encoding='utf-8') as writer:
        json.dump(data, writer)
    print(f"✅ Data dari {topic} disimpan ke HDFS: {hdfs_path}{filename}")

print("Consumer siap, menunggu pesan...")
for message in consumer:
    data = message.value
    save_to_hdfs(message.topic, data)