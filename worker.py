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
system keeps running throughout. A redelivery is provable, not just
inferred: the second delivery arrives with the broker's
``method.redelivered`` flag set to True, and the worker logs it.

Duplicate handling (atomic)
---------------------------
Processed ``event_id`` values are recorded in a shared JSON file
(default ``processed_events.json`` in the working directory) as
``event_id -> {"status": "processing" | "processed", "worker": ..., "pid": ...}``.

The check and the claim are ONE atomic step: under an exclusive file
lock, the worker re-reads the store and either claims the event (status
``processing``) or finds it already claimed/processed. Two simultaneous
copies therefore cannot both run the business action.

Crucially, a copy that arrives while another worker is still processing
is NOT ACKed straight away. ACKing it would discard the only backup
copy: if the first worker then crashed before finishing, that copy
would be gone too, and the business action could be lost. Instead the
second worker holds its copy unacknowledged and polls the claim: if the
first worker finishes, the copy is ACKed as a duplicate; if the first
worker's PID dies, the waiting worker reclaims the event and processes
it itself. Only after 30 seconds of no resolution does it NACK with
requeue=True, which also preserves the message. A ``processed`` record
is the only case ACKed immediately, because the work is already done.
A claim is not a trap for crash redelivery: if the claiming worker's
PID is dead, the redelivered copy reclaims the event and processes it
normally.
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


def load_store(store_path):
    """Return {event_id: {status, worker, pid}}; tolerate the old list format."""
    try:
        with open(store_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    if isinstance(data, list):
        return {eid: {"status": "processed", "worker": "unknown", "pid": None}
                for eid in data}
    return data if isinstance(data, dict) else {}


def _save_store_locked(store_path, store):
    tmp_path = store_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, sort_keys=True)
    os.replace(tmp_path, store_path)


def pid_alive(pid):
    """True only if pid is a live, non-zombie process.

    ``os.kill(pid, 0)`` alone is not enough: a crashed worker whose
    parent has not reaped it yet is a zombie, and kill() still succeeds
    on a zombie. On Linux, check /proc/<pid>/status for state Z.
    """
    if not isinstance(pid, int):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("State:"):
                    return "Z" not in line.split(":", 1)[1]
    except FileNotFoundError:
        return False
    except OSError:
        pass
    return True


def try_claim(store_path, event_id, worker_name):
    """Atomically claim event_id, or report why it is a duplicate.

    Returns (outcome, entry) where outcome is one of:
    "claimed", "reclaimed", "duplicate-processed", "duplicate-processing".
    The claim write happens inside the same exclusive lock as the check,
    so check-then-process cannot race across worker processes.
    """
    lock_path = store_path + ".lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            store = load_store(store_path)
            entry = store.get(event_id)
            if entry and entry.get("status") == "processed":
                return "duplicate-processed", entry
            if entry and entry.get("status") == "processing":
                if pid_alive(entry.get("pid")):
                    return "duplicate-processing", entry
                # Claiming worker is dead (crash before ACK): reclaim.
                store[event_id] = {"status": "processing",
                                   "worker": worker_name,
                                   "pid": os.getpid()}
                _save_store_locked(store_path, store)
                return "reclaimed", entry
            store[event_id] = {"status": "processing",
                               "worker": worker_name,
                               "pid": os.getpid()}
            _save_store_locked(store_path, store)
            return "claimed", None
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def wait_for_resolution(store_path, event_id, worker_name, timeout=30.0):
    """Wait for a live claim to resolve, without ACKing the held copy.

    Returns "duplicate-processed" (first worker finished: safe to ACK),
    "reclaimed" (first worker died: this worker now holds the claim),
    "duplicate-processing" (another waiting worker reclaimed it first:
    keep waiting), "missing" (claim vanished: re-claim), or "timeout"
    (still unresolved after ``timeout`` seconds: caller must NACK with
    requeue=True, never ACK, so the copy is not lost).
    The held copy stays unacknowledged throughout, so if this worker
    itself crashes while waiting, RabbitMQ redelivers its copy too.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        entry = load_store(store_path).get(event_id)
        if entry is None:
            return "missing"
        if entry.get("status") == "processed":
            return "duplicate-processed"
        if entry.get("status") == "processing" and not pid_alive(entry.get("pid")):
            outcome, _entry = try_claim(store_path, event_id, worker_name)
            return outcome
        time.sleep(0.2)
    return "timeout"


def mark_processed(store_path, event_id, worker_name):
    """Mark event_id processed, under the same exclusive lock."""
    lock_path = store_path + ".lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            store = load_store(store_path)
            store[event_id] = {"status": "processed",
                               "worker": worker_name,
                               "pid": os.getpid()}
            _save_store_locked(store_path, store)
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
    parser.add_argument("--fail-during", default=None,
                        help="event_id to crash on DURING processing, after "
                             "claiming but before marking processed or ACKing "
                             "(loss-protection demo).")
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
            f"item_id={message['item_id']} quantity={message['quantity']} "
            f"redelivered={method.redelivered}")
        if method.redelivered:
            log(args.name, f"REDELIVERY: broker redelivered flag is True "
                           f"for event_id={event_id} "
                           f"-- this is a broker redelivery, not a new publish")

        # Atomic duplicate check + claim (see try_claim).
        outcome, entry = try_claim(args.store, event_id, args.name)
        if outcome == "duplicate-processed":
            log(args.name, f"DUPLICATE detected for event_id={event_id} "
                           f"-- already processed by {entry.get('worker')} "
                           f"-- skipping business action, ACKing to drop it")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        if outcome == "duplicate-processing":
            # DO NOT ACK yet: this copy is the backup if the first
            # worker crashes. Hold it unacknowledged and poll the claim.
            log(args.name, f"DUPLICATE detected for event_id={event_id} "
                           f"-- already being processed by {entry.get('worker')} "
                           f"(pid {entry.get('pid')} still alive) "
                           f"-- NOT ACKing yet: holding this copy unacked "
                           f"while the first worker resolves, so it is not "
                           f"lost if the first worker crashes")
            while True:
                result = wait_for_resolution(args.store, event_id, args.name)
                if result == "duplicate-processed":
                    log(args.name, f"DUPLICATE resolved for event_id={event_id} "
                                   f"-- first worker finished "
                                   f"-- now ACKing to drop it")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                if result == "reclaimed":
                    log(args.name, f"RECLAIMED event_id={event_id} "
                                   f"-- first worker died while processing, "
                                   f"so this waiting copy will be processed")
                    break
                if result == "timeout":
                    log(args.name, f"DUPLICATE unresolved for event_id={event_id} "
                                   f"after 30s -- NACKing with requeue=True "
                                   f"so this copy is NOT lost")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                if result == "missing":
                    outcome2, _ = try_claim(args.store, event_id, args.name)
                    if outcome2 in ("claimed", "reclaimed"):
                        break
                    continue
                # Another waiting worker reclaimed it first: keep waiting.
                continue
        if outcome == "reclaimed":
            log(args.name, f"RECLAIMED event_id={event_id} "
                           f"-- previous claim by {entry.get('worker')} "
                           f"(pid {entry.get('pid')}) is dead, "
                           f"so this redelivery will be processed")

        # Failure simulation: die before ACK. The event stays claimed
        # with a dead PID, so its redelivery reclaims it (see above).
        if args.fail_on and event_id == args.fail_on:
            log(args.name, f"SIMULATED CRASH on event_id={event_id} "
                           f"-- exiting WITHOUT ACK (message will be redelivered)")
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(1)

        # The business action (here: simulated order processing).
        # No other worker can be running it: this worker holds the claim.
        log(args.name, f"processing order {message['order_id']} ...")
        # Failure simulation, mid-processing: crash after claiming and
        # starting work, but before marking processed or ACKing. Any
        # worker holding a duplicate copy must take over (see above).
        if args.fail_during and event_id == args.fail_during:
            time.sleep(0.5)
            log(args.name, f"SIMULATED CRASH DURING PROCESSING on event_id={event_id} "
                           f"-- exiting WITHOUT ACK and WITHOUT marking processed")
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(1)
        time.sleep(args.delay)
        mark_processed(args.store, event_id, args.name)
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
