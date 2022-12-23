from confluent_kafka import Consumer

# The auto.offset.reset property specifies what offset the consumer should start reading from
# in the event there are no committed offsets for a partition,
# or the committed offset is invalid (perhaps due to log truncation).
conf = {
    'bootstrap.servers': "host1:9092,host2:9092",
    'group.id': "foo",
    'auto.offset.reset': 'smallest'
}

consumer1 = Consumer(conf)

# This is another example with the enable.auto.commit configured to False in the consumer. The default value is True.
conf = {
    'bootstrap.servers': 'host1:9092,host2:9092',
    'group.id': "foo",
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest'
}

consumer2 = Consumer(conf)


# For information on the available configuration properties, refer to the API Documentation.
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
# https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md


def commit_completed(err, partitions):
    if err:
        print(str(err))
    else:
        print("Committed partition offsets: " + str(partitions))


conf = {
    'bootstrap.servers': "host1:9092,host2:9092",
    'group.id': "foo",
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    },
    'on_commit': commit_completed
}

consumer3 = Consumer(conf)
