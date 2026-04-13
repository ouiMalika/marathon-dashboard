"""Create the Kafka topic used by the marathon analytics pipeline.

Run this once after starting the Kafka broker, before launching the producer
or consumer. The topic name and partition count are read from config.
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import KAFKA_BOOTSTRAP, TOPIC, NUM_PARTITIONS


def main():
    """Create the topic, or report that it already exists."""
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="marathon-admin",
    )
    topic = NewTopic(
        name=TOPIC,
        num_partitions=NUM_PARTITIONS,
        replication_factor=1,
    )
    try:
        admin.create_topics([topic])
        print(f"Created topic '{TOPIC}' with {NUM_PARTITIONS} partitions.")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC}' already exists.")
    finally:
        admin.close()


if __name__ == "__main__":
    main()