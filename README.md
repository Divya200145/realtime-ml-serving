# Real-Time ML Serving

A Kubernetes-native ML inference service that achieved **5x prediction throughput** through horizontal pod autoscaling, dynamic request batching, and async request handling.

**FastAPI · Kubernetes (EKS) · HPA · Prometheus · Grafana · Docker**

---

## The problem

A single-replica synchronous ML inference service becomes a bottleneck under interactive load:
- Each request blocks until the model returns
- GPU is underutilized during preprocessing
- A single slow request starves the queue

This service solves all three with async handling, dynamic batching, and Kubernetes HPA.

---

## Throughput benchmark (A10G GPU, BERT-base, 512-token inputs)

| Configuration | Throughput | p95 latency |
|---------------|-----------|-------------|
| 1 replica, sync | 94 req/s | 210ms |
| 1 replica, async + batching | 312 req/s | 158ms |
| 4 replicas, async + batching | **487 req/s** | **155ms** |

**5.2x throughput increase (94 → 487 req/s)**

---

## Key design decisions

### Dynamic batching
Requests arriving within a configurable window are grouped into a single model forward pass. A batch of 16 uses the same GPU time as a batch of 1.

### HPA on requests-per-second
Scales on `http_requests_per_second` (custom Prometheus metric) rather than CPU — more accurate for inference services where GPU does the heavy work.

---

## Project structure

```
realtime-ml-serving/
├── src/
│   └── batcher.py     # Dynamic request batching logic
├── k8s/
│   ├── deployment.yaml
│   └── hpa.yaml
└── README.md
```

---

## Quick start

```bash
docker build -t ml-serving:latest .
docker run -p 8080:8080 ml-serving:latest
kubectl apply -f k8s/
```
