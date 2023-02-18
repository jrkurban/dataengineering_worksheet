from kafka import  KafkaConsumer

bootstrap_servers = ['localhost:9092', 'localhost:9292']


# Create a Kafka consumer
consumer = KafkaConsumer("topic1", bootstrap_servers=bootstrap_servers)

# Consume and print the messages from the Kafka topic  q6
for message in consumer:
    print(f"Key: {message.key.decode()}, Value: {message.value.decode()}, Partition: {message.partition}, TS: {message.timestamp}")


