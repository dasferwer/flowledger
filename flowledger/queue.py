import os

import pika


def connect():
    connection = pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"]))
    channel = connection.channel()
    channel.queue_declare(
        queue="changes", durable=True, arguments={"x-single-active-consumer": True}
    )
    return connection, channel
