from kafka.admin import NewTopic

def create_a_new_topic_if_not_exists(admin_client, topic_name="example-topic", num_partitions=1, replication_factor=1):
    # check if topic already exists
    topic_metadata = admin_client.list_topics(topic=topic_name)
    if topic_metadata:
        print(f"Topic '{topic_name}' already exists, no action taken")
        return

    # create topic if it doesn't exist
    topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    admin_client.create_topics([topic])
    print(f"Topic '{topic_name}' created with {num_partitions} partitions and a replication factor of {replication_factor}")