import requests
import time
import json
import os
import random
import threading


# Environment variables
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090/api/v1/query")
QUERY_LIST = json.loads(os.environ.get("QUERY_LIST", "[\"rate(metric_0[1m])\"]"))
LOG_FILE = os.environ.get("LOG_FILE", "query_logs.json")
QUERY_INTERVAL = float(os.environ.get("QUERY_INTERVAL", 1))
NUM_THREADS = int(os.environ.get("NUM_THREADS", 5))
EXPERIMENT_DURATION = int(os.environ.get("EXPERIMENT_DURATION", 60))
SEED = int(os.environ.get("SEED", 42))
print(f"Query list: {QUERY_LIST}")
random.seed(SEED)


class QueryComponent:
    def __init__(self):
        self.queries = QUERY_LIST
        self.interval = QUERY_INTERVAL
        self.num_threads = NUM_THREADS
        self.duration = EXPERIMENT_DURATION
        self.log_file = LOG_FILE
        self.logs = []
        self.log_lock = threading.Lock()
        print(f"Queries: {self.queries}")
        print(f"type of queries: {type(self.queries)}")

    def execute_query(self, query):
        t0 = int(time.time() * 1000)
        try:
            response = requests.get(PROMETHEUS_URL, params={"query": query}, timeout=60)
            t1 = int(time.time() * 1000)
            latency_ms = response.elapsed.total_seconds() * 1000
            result = {
                "query": query,
                "request_timestamp_ms": t0,
                "respond_timestamp_ms": t1,
                "status_code": response.status_code,
                "latency_ms": latency_ms
            }
            if response.status_code == 200:
                result["data"] = response.json()
            else:
                result["error"] = f"Non-200 response: {response.status_code}"
            return result
        except requests.exceptions.Timeout:
            t1 = int(time.time() * 1000)
            return {
                "query": query,
                "request_timestamp_ms": t0,
                "respond_timestamp_ms": t1,
                "status_code": 408,
                "latency_ms": t1 - t0,
                "error": "Query timed out"
            }
        except requests.exceptions.RequestException as e:
            t1 = int(time.time() * 1000)
            return {
                "query": query,
                "request_timestamp_ms": t0,
                "respond_timestamp_ms": t1,
                "status_code": 500,
                "latency_ms": None,
                "error": str(e)
            }

    def query_worker(self):
        thread_start_time = time.time()
        while time.time() - thread_start_time < self.duration:
            query = random.choice(self.queries)
            result = self.execute_query(query)

            self.log_lock.acquire()
            try:
                self.logs.append(result)
            finally:
                self.log_lock.release()

            time.sleep(self.interval)

    def run(self):
        threads = []

        for _ in range(self.num_threads):
            t = threading.Thread(target=self.query_worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        with open(self.log_file, 'w') as f:
            json.dump(self.logs, f, indent=4)

if __name__ == "__main__":
    component = QueryComponent()
    component.run()
    print("Query execution completed.")
