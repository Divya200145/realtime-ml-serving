# 📡 Real-Time ML Serving

> High-throughput, low-latency ML inference system with **sub-100ms p95 latency** at 10k+ RPS, built on FastAPI, Apache Kafka, and Kubernetes — with full Prometheus/Grafana observability.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat-square&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## 🧩 Problem Statement

Most ML serving setups are either too slow for real-time use cases (batch-oriented) or too fragile for production (single-instance, no autoscaling). This system handles streaming inference at scale — decoupled event-driven architecture with Kafka, async FastAPI serving, dynamic batching, and HPA-based autoscaling on Kubernetes.

---

## 🏗️ Architecture

```
Upstream Events (Clickstream, IoT, API)
            │
            ▼
  ┌──────────────────┐
  │   Apache Kafka    │   ◄── topic partitioned by model type
  │  (input topics)   │
  └────────┬─────────┘
           │ consume
  ┌────────▼─────────────────────────────┐
  │         FastAPI Inference Server      │
  │                                       │
  │  ┌─────────────┐  ┌───────────────┐  │
  │  │ Dynamic      │  │  Model Cache  │  │
  │  │ Batcher      │  │  (in-memory)  │  │
  │  └──────┬──────┘  └───────────────┘  │
  │         │                             │
  │  ┌──────▼──────────────────────────┐ │
  │  │   Model Inference (ONNX/TorchScript) │
  │  └──────┬──────────────────────────┘ │
  └─────────┼────────────────────────────┘
            │ publish predictions
  ┌─────────▼─────────┐
  │   Kafka (output)   │  ──► Downstream consumers
  └───────────────────┘
            │
  ┌─────────▼─────────────────┐
  │   Prometheus + Grafana     │
  │  (latency, throughput,     │
  │   error rate, queue lag)   │
  └───────────────────────────┘
            │
  ┌─────────▼─────────────────┐
  │  Kubernetes HPA            │
  │  (auto-scale on RPS + CPU) │
  └───────────────────────────┘
```

---

## ⚡ Key Results

| Metric | Value |
|---|---|
| p95 inference latency | **< 100ms** |
| Peak throughput | **10,000+ RPS** |
| Model load time (cold start) | **< 3 seconds** |
| Autoscaling response time | **< 90 seconds** |
| XGBoost latency reduction vs baseline | **15%** |

---

## 🛠️ Tech Stack

- **Serving**: FastAPI (async), Uvicorn workers, dynamic request batching
- **Models**: ONNX Runtime, TorchScript, XGBoost, scikit-learn
- **Streaming**: Apache Kafka (confluent-kafka-python)
- **Orchestration**: Kubernetes, Horizontal Pod Autoscaler (HPA)
- **Observability**: Prometheus (custom metrics), Grafana dashboards
- **Infrastructure**: AWS EKS, Terraform
- **CI/CD**: GitHub Actions

---

## 🚀 Quickstart

```bash
# Clone and install
git clone https://github.com/Divya200145/realtime-ml-serving.git
cd realtime-ml-serving
pip install -r requirements.txt

# Start local Kafka (Docker Compose)
docker-compose up -d kafka zookeeper

# Export your model to ONNX (example)
python scripts/export_model.py --model models/classifier.pkl --output models/classifier.onnx

# Start the inference server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

# Or start the Kafka consumer mode
python app/consumer.py --topic ml-input --model models/classifier.onnx

# Run load test
python scripts/load_test.py --rps 1000 --duration 60
```

---

## 📁 Project Structure

```
realtime-ml-serving/
├── app/
│   ├── main.py              # FastAPI app & REST inference endpoint
│   ├── consumer.py          # Kafka consumer + inference loop
│   ├── batcher.py           # Dynamic request batching logic
│   ├── model_registry.py    # Model loading & caching
│   └── metrics.py           # Prometheus instrumentation
├── scripts/
│   ├── export_model.py      # Export sklearn/PyTorch → ONNX
│   └── load_test.py         # Throughput & latency benchmarking
├── k8s/
│   ├── deployment.yaml      # Kubernetes deployment
│   └── hpa.yaml             # Horizontal Pod Autoscaler config
├── infra/
│   └── terraform/           # AWS EKS IaC
├── tests/
├── docker-compose.yaml      # Local Kafka + Zookeeper setup
├── requirements.txt
└── Dockerfile
```

---

## ⚙️ Dynamic Batching

The batcher groups concurrent requests within a configurable time window, reducing per-request overhead:

```python
# config.yaml
batching:
  max_batch_size: 64
  max_wait_ms: 10        # collect requests for up to 10ms
  min_batch_size: 1      # process immediately if no more requests
```

This alone accounts for ~40% of the throughput improvement over naive single-request serving.

---

## 📈 Observability

Prometheus metrics exposed at `/metrics`:
- `inference_latency_seconds` — request latency histogram (p50/p95/p99)
- `inference_throughput_rps` — requests per second gauge
- `kafka_consumer_lag` — per-partition lag for backpressure monitoring
- `model_cache_hit_rate` — model cache effectiveness

Import `grafana/serving-dashboard.json` for the complete monitoring dashboard.

---

## 📄 License

MIT © Bala Divya
