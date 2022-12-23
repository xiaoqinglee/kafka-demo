import sys

from confluent_kafka import KafkaError, KafkaException, OFFSET_BEGINNING

MIN_COMMIT_COUNT = 4200


def reset_offset(consumer, partitions):
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def hook_func(consumer, partitions):
    # 按需
    reset_offset(consumer, partitions)


def msg_process(msg):
    print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics, on_assign=hook_func)
        msg_count = 0

        while True:
            # If no records are received before this timeout expires,
            # then Consumer.poll() will return an empty record set.
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # Initial message consumption may take up to `session.timeout.ms`
                # for the consumer group to rebalance and start consuming.
                print("Waiting...")
                continue
            elif msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    # Asynchronous Commits
                    #
                    # In this example,
                    # the consumer sends the request and returns immediately by using asynchronous commits.
                    # The asynchronous parameter to commit() is changed to True.
                    # The value is passed in explicitly,
                    # but asynchronous commits are the default if the parameter is not included.
                    #
                    # The API gives you a callback which is invoked when the commit either succeeds or fails.
                    # The commit callback can be any callable and can be passed as a configuration parameter
                    # to the consumer constructor.
                    consumer.commit(asynchronous=True)
    except KeyboardInterrupt:
        pass
    finally:
        # Note that you should always call Consumer.close() after you are finished using the consumer.
        # Doing so will ensure that active sockets are closed and internal state is cleaned up.
        # It will also trigger a group rebalance immediately
        # which ensures that any partitions owned by the consumer are re-assigned to another member in the group.
        # If not closed properly,
        # the broker will trigger the rebalance only after the session timeout has expired.

        # Close down consumer to commit final offsets.
        consumer.close()
