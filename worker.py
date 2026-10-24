"""Order Worker: consumes order messages from a RabbitMQ queue.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2 --fail-on evt-crash-001

Competing consumers
-------------------
Run two (or more) workers against the same queue with
``basic_qos(prefetch_count=1)``. RabbitMQ then hands each worker at most
one unacknowledged message at a time and round-robins the rest, so every
message is delivered to exactly one worker at a time -- never to both.

Failure and redelivery
----------------------
``--fail-on <event_id>`` simulates a crash: when that message arrives
the worker logs it and exits *without* sending an ACK and *without*
marking the event processed. RabbitMQ then requeues the message and
delivers it to another live worker, which processes it normally. The
system keeps running throughout.

Duplicate handling
------------------
Processed ``event_id`` values are recorded in a shared JSON file
(default ``processed_events.json`` in the working directory). The file
is re-read for every message and updated under an exclusive file lock,
so two worker processes cannot race each other. If an ``event_id`` is
already recorded, the business action is NOT run again; the worker logs
the duplicate and ACKs the message so it is not redelivered forever.
"""

import argparse
import fcntl
import json
import os
import sys
import time
from datetime import datetime

import pika


def log(name, message):
    print(f"[{name} {datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def load_processed(store_path):
    try:
        with open(store_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data) if isinstance(data, list) else set()
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def mark_processed(store_path, event_id):
    """Append event_id to the shared store under an exclusive lock."""
    lock_path = store_path + ".lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            processed = load_processed(store_path)
            processed.add(event_id)
            tmp_path = store_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(sorted(processed), f, indent=2)
            os.replace(tmp_path, store_path)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def main():
    parser = argparse.ArgumentParser(description="Consume order messages.")
    parser.add_argument("--name", default="worker-1")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--queue", default="orders")
    parser.add_argument("--store", default="processed_events.json",
                        help="Shared file of processed event_ids.")
    parser.add_argument("--fail-on", default=None,
                        help="event_id to crash on before ACK (redelivery demo).")
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
            event_id = message["event_id"]
        except (json.JSONDecodeError, KeyError) as exc:
            log(args.name, f"malformed message, discarding: {body!r} ({exc})")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        log(args.name,
            f"received event_id={event_id} order_id={message['order_id']} "
            f"item_id={message['item_id']} quantity={message['quantity']}")

        # Duplicate check: re-read the shared store for every message,
        # so a duplicate is caught even if another worker processed it.
        if event_id in load_processed(args.store):
            log(args.name, f"DUPLICATE detected for event_id={event_id} "
                           f"-- skipping business action, ACKing to drop it")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Failure simulation: die before ACK, before marking processed.
        # RabbitMQ requeues the message for another worker.
        if args.fail_on and event_id == args.fail_on:
            log(args.name, f"SIMULATED CRASH on event_id={event_id} "
                           f"-- exiting WITHOUT ACK (message will be redelivered)")
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(1)

        # The business action (here: simulated order processing).
        log(args.name, f"processing order {message['order_id']} ...")
        time.sleep(args.delay)
        mark_processed(args.store, event_id)
        log(args.name, f"processed order {message['order_id']} "
                       f"(event_id={event_id}), sending ACK")
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
