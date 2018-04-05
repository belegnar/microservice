from prometheus_client import Gauge, Histogram, Counter, Summary


class Metrics:
    requests_per_node = Counter("requests_per_node", "Number of requests per second per node")
    requests_total = Counter("requests_total", "Number of requests per second * number of nodes")
    latency = Gauge("latency", "Total time of request processed")
    transactions = Gauge("transactions", "Number of transactions")
