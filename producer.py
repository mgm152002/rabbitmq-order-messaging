"""Order Producer: publishes order messages to a RabbitMQ queue.

Usage:
    python producer.py --count 5
    python producer.py --event-id evt-crash-001 --order-id ORD-9001 --item-id widget --quantity 2
    python producer.py --event-id evt-dup-001 --order-id ORD-9002 --item-id widget --quantity 1 --repeat 2

Every message is a JSON object with exactly the fields the assignment
requires, plus nothing else:

    {"event_id": ..., "order_id": ..., "item_id": ..., "quantity": ...}

Messages are published as persistent (delivery_mode=2) to a durable
queue, so they survive a broker restart while they are still queued.
"""

import argparse
import json
import sys
import uuid

import pika


def connect(host, port):
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port)
    )


def publish(channel, queue, message):
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),  # persistent
    )
    print(
        f"[producer] published event_id={message['event_id']} "
        f"order_id={message['order_id']} item_id={message['item_id']} "
        f"quantity={message['quantity']}",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Publish order messages.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--queue", default="orders")
    parser.add_argument("--count", type=int, default=5,
                        help="Messages to publish when no explicit event_id is given.")
    parser.add_argument("--event-id", default=None,
                        help="Publish a single message with this event_id.")
    parser.add_argument("--order-id", default=None)
    parser.add_argument("--item-id", default="widget")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=1,
                        help="Publish the same message this many times "
                             "(use 2 to create a duplicate event_id).")
    args = parser.parse_args()

    connection = connect(args.host, args.port)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=args.queue, durable=True)

        if args.event_id:
            message = {
                "event_id": args.event_id,
                "order_id": args.order_id or f"ORD-{args.event_id}",
                "item_id": args.item_id,
                "quantity": args.quantity,
            }
            for _ in range(args.repeat):
                publish(channel, args.queue, message)
        else:
            for i in range(1, args.count + 1):
                publish(channel, args.queue, {
                    "event_id": str(uuid.uuid4()),
                    "order_id": f"ORD-{1000 + i}",
                    "item_id": args.item_id,
                    "quantity": args.quantity,
                })
    finally:
        connection.close()


if __name__ == "__main__":
    sys.exit(main())
