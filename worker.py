"""Order Worker: consumes order messages from a RabbitMQ queue.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2

Competing consumers
-------------------
Run two (or more) workers against the same queue with
``basic_qos(prefetch_count=1)``. RabbitMQ then hands each worker at most
one unacknowledged message at a time and round-robins the rest, so every
message is delivered to exactly one worker at a time -- never to both.

Messages are acknowledged manually, only after processing finishes.
"""

import argparse
import json
import sys
import time
from datetime import datetime

import pika


def log(name, message):
    print(f"[{name} {datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Consume order messages.")
    parser.add_argument("--name", default="worker-1")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--queue", default="orders")
    parser.add_argument("--delay", type=float, default=0.8,
                        help="Simulated processing time in seconds.")
    args = parser.parse_args()

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=args.host, port=args.port)
    )
    channel = connection.channel()
    channel.queue_declare(queue=args.queue, durable=True)
    # Fair dispatch: one unacknowledged message per worker at a time.
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, _properties, body):
        try:
            message = json.loads(body)
        except json.JSONDecodeError:
            log(args.name, f"malformed message, discarding: {body!r}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        log(args.name,
            f"received event_id={message['event_id']} "
            f"order_id={message['order_id']} item_id={message['item_id']} "
            f"quantity={message['quantity']}")
        log(args.name, f"processing order {message['order_id']} ...")
        time.sleep(args.delay)
        log(args.name, f"processed order {message['order_id']} "
                       f"(event_id={message['event_id']}), sending ACK")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=args.queue, on_message_callback=on_message,
                          auto_ack=False)
    log(args.name, f"waiting for messages on queue '{args.queue}' "
                   f"(prefetch=1, manual ACK)")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log(args.name, "shutting down")
        channel.stop_consuming()
    connection.close()


if __name__ == "__main__":
    sys.exit(main())
