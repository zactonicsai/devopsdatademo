#!/usr/bin/env python3
"""
kafka_api.py — lightweight health/test/metrics API for the zacamikafka instance.

Serves the dashboard and three JSON endpoints:
  GET  /api/health   -> broker reachability, topic count, uptime
  GET  /api/metrics  -> cumulative + rolling produce/consume metrics
  POST /api/test     -> run a produce->consume round-trip test, return timing

Uses only the Python stdlib HTTP server plus confluent-kafka (installed in the
zacamikafka AMI). Designed for single-node KRaft broker on localhost:9092.
Reachable via SSM port-forwarding only — binds to 127.0.0.1.
"""
import json
import os
import time
import threading
import statistics
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
LISTEN_HOST = os.environ.get("API_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("API_PORT", "8080"))
DASHBOARD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
START_TS = time.time()

# Latency bucket upper bounds in ms (matches dashboard labels)
BUCKETS = [1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64, float("inf")]

# ---------------- shared metrics state ----------------
_lock = threading.Lock()
metrics = {
    "total_sent": 0,
    "total_received": 0,
    "total_errors": 0,
    "recent_latencies": [],   # last N round-trip latencies (ms)
    "last_test": None,
    "histogram": [0] * len(BUCKETS),
    "last_throughput": 0.0,
    "last_recv_throughput": 0.0,
}
RECENT_MAX = 2000


def admin():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def broker_health():
    """Return (up, topic_count)."""
    try:
        md = admin().list_topics(timeout=4)
        return True, len(md.topics)
    except Exception:
        return False, 0


def ensure_topic(topic):
    try:
        a = admin()
        md = a.list_topics(timeout=4)
        if topic in md.topics:
            return
        fs = a.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        for _, f in fs.items():
            try:
                f.result(timeout=8)
            except Exception:
                pass  # already exists / race
    except Exception:
        pass


def bucket_index(ms):
    for i, b in enumerate(BUCKETS):
        if ms <= b:
            return i
    return len(BUCKETS) - 1


def run_test(count, size, topic):
    """Produce `count` messages then consume them back, measuring round-trip latency."""
    ensure_topic(topic)
    payload_pad = b"x" * max(0, size - 24)  # leave room for the timestamp prefix

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 5,
        "acks": "1",
    })
    group = f"healthcheck-{int(time.time()*1000)}"
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    # prime the consumer assignment so we don't miss early messages
    consumer.poll(1.0)

    send_ts = {}
    latencies = []
    errors = 0
    sent = 0

    t0 = time.time()
    for i in range(count):
        key = str(i).encode()
        now = time.time()
        send_ts[i] = now
        # encode index + send time so the consumer can compute round-trip
        msg = f"{i}:{now:.6f}:".encode() + payload_pad
        try:
            producer.produce(topic, key=key, value=msg)
            sent += 1
        except BufferError:
            producer.poll(0.1)
            producer.produce(topic, key=key, value=msg)
            sent += 1
        except Exception:
            errors += 1
        if i % 500 == 0:
            producer.poll(0)
    producer.flush(15)

    # consume back
    received = 0
    deadline = time.time() + max(10, count / 1000 + 5)
    while received < sent and time.time() < deadline:
        m = consumer.poll(0.5)
        if m is None:
            continue
        if m.error():
            continue
        try:
            parts = m.value().split(b":", 2)
            idx = int(parts[0])
            sts = float(parts[1])
            rtt = (time.time() - sts) * 1000.0
            latencies.append(rtt)
            received += 1
        except Exception:
            received += 1
    consumer.close()
    duration = time.time() - t0

    # stats
    latencies.sort()
    avg = statistics.mean(latencies) if latencies else 0.0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0.0
    tput = sent / duration if duration > 0 else 0.0
    rtput = received / duration if duration > 0 else 0.0

    histo = [0] * len(BUCKETS)
    for l in latencies:
        histo[bucket_index(l)] += 1

    with _lock:
        metrics["total_sent"] += sent
        metrics["total_received"] += received
        metrics["total_errors"] += errors
        metrics["recent_latencies"].extend(latencies)
        del metrics["recent_latencies"][:-RECENT_MAX]
        metrics["histogram"] = histo
        metrics["last_throughput"] = tput
        metrics["last_recv_throughput"] = rtput
        metrics["last_test"] = {
            "sent": sent, "received": received, "errors": errors,
            "duration_ms": duration * 1000.0,
            "avg_latency_ms": avg, "p95_latency_ms": p95, "p99_latency_ms": p99,
            "throughput_msg_s": tput, "recv_throughput_msg_s": rtput,
            "histogram": histo,
        }

    return metrics["last_test"]


def current_metrics():
    with _lock:
        lat = list(metrics["recent_latencies"])
        lat.sort()
        avg = statistics.mean(lat) if lat else None
        p95 = lat[int(len(lat) * 0.95)] if lat else None
        err_rate = (metrics["total_errors"] / metrics["total_sent"] * 100.0) if metrics["total_sent"] else 0.0
        return {
            "total_sent": metrics["total_sent"],
            "total_received": metrics["total_received"],
            "avg_latency_ms": avg,
            "p95_latency_ms": p95,
            "throughput_msg_s": metrics["last_throughput"],
            "recv_throughput_msg_s": metrics["last_recv_throughput"],
            "error_rate_pct": err_rate,
            "histogram": metrics["histogram"],
        }


def uptime_str():
    s = int(time.time() - START_TS)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):
        pass  # quiet

    def do_GET(self):
        if self.path in ("/", "/index.html", "/dashboard.html"):
            try:
                with open(DASHBOARD, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(404, {"error": "dashboard.html not found"})
        elif self.path.startswith("/api/health"):
            up, topics = broker_health()
            self._send(200, {"broker_up": up, "topics": topics,
                             "bootstrap": BOOTSTRAP, "uptime": uptime_str()})
        elif self.path.startswith("/api/metrics"):
            self._send(200, current_metrics())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path.startswith("/api/test"):
            try:
                ln = int(self.headers.get("Content-Length", 0))
                req = json.loads(self.rfile.read(ln) or b"{}")
                count = max(1, min(int(req.get("count", 100)), 100000))
                size = max(1, min(int(req.get("size", 256)), 1048576))
                topic = str(req.get("topic", "health-check"))[:200] or "health-check"
                result = run_test(count, size, topic)
                self._send(200, result)
            except KafkaException as e:
                self._send(200, {"error": f"kafka: {e}"})
            except Exception as e:
                self._send(200, {"error": str(e)})
        else:
            self._send(404, {"error": "not found"})


def main():
    srv = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(f"zacamikafka API on http://{LISTEN_HOST}:{LISTEN_PORT}  (broker {BOOTSTRAP})", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
