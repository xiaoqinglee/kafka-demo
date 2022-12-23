from confluent_kafka import Producer
import socket

conf = {
    'bootstrap.servers': "host1:9092,host2:9092",
    'client.id': socket.gethostname()
}

producer = Producer(conf)


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


# Asynchronous writes
#
topic = "topic_foo"

# The produce call will complete immediately and does not return a value.
# A KafkaException will be thrown
# if the message could not be enqueued due to librdkafka’s local produce queue being full.

# Although the produce() method enqueues message immediately for batching, compression and transmission to broker,
# no delivery notification events will be propagated until poll() is invoked.
producer.produce(topic, key="key", value="value", callback=acked)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(1)

# Synchronous writes
#
# The Python client provides a flush() method which can be used to make writes synchronous.
# This is typically a bad idea since it effectively limits throughput to the broker round trip time,
# but may be justified in some cases.
producer.produce(topic, key="key", value="value", callback=acked)
producer.flush()
