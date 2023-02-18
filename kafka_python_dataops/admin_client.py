from kafka.admin import KafkaAdminClient, NewTopic
import time

admin_client = KafkaAdminClient(bootstrap_servers=['127.0.0.1:9092'],
                                client_id='dataops_client')
##list topics
print("Before create topics", admin_client.list_topics())

##create topics

try:
    topic2 = NewTopic(name='topic2', num_partitions=4, replication_factor=3)
    topic3 = NewTopic(name='topic3', num_partitions=2, replication_factor=2)
    topic4 = NewTopic(name='topic4', num_partitions=2, replication_factor=2)

    admin_client.create_topics(new_topics=[topic2, topic3, topic4])
except:
    print("Topics are already exist.")



##list topics
time.sleep(2)
print("After create topics", admin_client.list_topics())

##delete topics
admin_client.delete_topics(topics=['topic2', 'topic3'])

##list topics
time.sleep(2)
print("After delete topics", admin_client.list_topics())

##decribe topic

print("Describe topics", admin_client.describe_topics(topics=['topic4']))

##close client

admin_client.close()
