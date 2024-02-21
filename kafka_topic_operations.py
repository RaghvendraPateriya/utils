"""
Kafka Topic Opeations: List, Create & Delete topic.

Requirement: pip install kafka-python
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

def list_topic() -> list:
    existing_topic_list = admin_client.list_topics()
    print("Topic Count: ", len(existing_topic_list))
    print(existing_topic_list)
    return existing_topic_list

def create_topics(topic_names: list) -> None:

    existing_topic_list = list_topic()
    print(existing_topic_list)
    topic_list = []
    for topic in topic_names:
        if topic not in existing_topic_list:
            print(f'Topic : {topic} added ')
            topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        else:
            print(f'Topic : {topic} already exist ')
    try:
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic Created Successfully")
        else:
            print("Topic Exist")
    except TopicAlreadyExistsError as e:
        print("Topic Already Exist")
    except  Exception as e:
        print(e)

def delete_topics(topic_names) -> None:
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError as e:
        print("Topic Doesn't Exist")
    except  Exception as e:
        print(e)


if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    list_topic()
    # create_topics(["local.barco.edu.anomalies.raw.v1"])
    # delete_topics(["local.barco.edu.anomalies.raw.v1"])
    #list_topic()
