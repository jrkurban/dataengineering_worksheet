from kafka import KafkaProducer
import time

# Define the Kafka topic and bootstrap server
bootstrap_servers = ['localhost:9092', 'localhost:9292']

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         client_id='procuder')

# Define the regions and their keys
regions = {
    1: 'Marmara',
    2: 'Ege',
    3: 'Akdeniz',
    4: 'İç Anadolu',
    5: 'Doğu Anadolu',
    6: 'Güneydoğu Anadolu',
    7: 'Karadeniz'
}

# Produce the regions to the Kafka topic with their keys as partition keys
for key, value in regions.items():
    producer.send("topic1", key=str(key).encode("utf-8"), value=value.encode("utf-8"))
    time.sleep(1)  # sleep for 1 second between each message