# Báo cáo đề xuất kiến trúc hệ thống thu thập và phân tích dữ liệu tiền ảo

## 1. Mục tiêu

- Xây dựng nền tảng thu thập, lưu trữ, xử lý và cung cấp dữ liệu thị trường tiền mã hóa theo thời gian thực và theo lô (batch) phục vụ phân tích, cảnh báo rủi ro và mô hình hoá (ML) cho mục tiêu nghiên cứu/học thuật và triển khai doanh nghiệp.
- Đáp ứng yêu cầu: chịu lỗi, mở rộng theo chiều ngang, truy vấn nhanh, audit và tái tính toán lịch sử (recompute).

## 2. Yêu cầu chức năng chính

- Thu thập dữ liệu thị trường: giá, orderbook, trade stream, on-chain, social sentiment.
- Đảm bảo độ chính xác và thứ tự thời gian (timestamps, watermarking).
- Lưu trữ raw data cho replay (HDFS/object storage).
- Hỗ trợ phân tích batch (historical backtests) và streaming (real-time indicators, alerts).
- API phục vụ truy vấn nhanh cho dashboard và downstream services.

## 3. Yêu cầu phi chức năng

- Khả năng mở rộng: scale-out cho Kafka, Spark, HDFS, và DB phục vụ.
- Độ trễ: streaming path mục tiêu < 1–5s từ nguồn đến alert (tùy use-case).
- Độ tin cậy: replication thuần tuý cho Kafka (RF=3), HDFS replication >= 3.
- Bảo mật: TLS in-transit, encryption at-rest, RBAC cho dịch vụ.
- Giám sát: metrics (Prometheus), logs (ELK), tracing (Jaeger), alerting (Grafana Alerts).

## 4. Tổng quan kiến trúc (Kiến trúc Lambda đề xuất)

- Collection Layer: collectors (CoinGecko REST, Binance WebSocket, Twitter stream, on-chain indexers) chịu trách nhiệm ingest dữ liệu raw và push ra Kafka topics.
- Ingestion / Speed Layer: Apache Kafka làm backbone message bus cho streaming; producers ghi events, stream processors hoặc Spark Structured Streaming tiêu thụ.
- Batch Layer: HDFS (hoặc S3-like object storage) chứa immutable raw + curated datasets; Spark batch jobs chạy offline để rebuild views và huấn luyện mô hình.
- Serving Layer: HBase / Cassandra cho bảng time-series đã xử lý; Redis làm cache cho truy vấn thấp-latency; REST API (FastAPI) để phục vụ dashboard & downstream.
- Orchestration: Apache Airflow để điều phối batch ETL, huấn luyện model, và nhiệm vụ bảo trì.
- Infra: Kubernetes (container orchestration) chạy microservices (collectors, APIs, sidecars), Kafka & Zookeeper có thể chạy trên K8s hoặc VM/managed service.

ASCII Diagram (tóm tắt):

Producer(s) -> Kafka -> (Spark Streaming) -> Serving DB (HBase/Redis) -> API -> Dashboard
|\
 | \-> Batch Sink -> HDFS -> Spark Batch -> Serving DB

## 5. Thành phần chi tiết & lý do chọn

- Kafka (3 brokers, RF=3): messaging backbone, hỗ trợ replay, partitioning theo symbol, độ bền cao.
- Spark (PySpark + Structured Streaming): xử lý cả batch và streaming với API thống nhất; hỗ trợ window, watermark, stateful operations.
- HDFS (hoặc S3): lưu raw data và parquet partitioned cho hiệu năng batch.
- HBase (hoặc Cassandra): serving store cho time-series với random reads/point lookups và scan theo key-range.
- Redis: cache TTL cho dashboard queries, leaderboard, và rate-limiting.
- Airflow: orchestration DAG cho batch pipelines + ML retraining.
- Kubernetes: deployment, scaling, config / secrets management (sealed secrets / Vault), rolling upgrades.
- Observability: Prometheus (metrics), Grafana (dashboard), ELK (logs), Jaeger (tracing).

## 6. Data flow chi tiết

1. Collector (Python service) kết nối WebSocket / REST, chuẩn hóa event và publish sang Kafka topic tương ứng (e.g., `trades`, `quotes`, `orderbook`, `social`).
2. Kafka giữ các topic partitioned theo symbol/timestamp; producers gửi với key=symbol.
3. Spark Structured Streaming tiêu thụ topics, thực hiện cleansing, dedup, windowed aggregations (SMA, EMA, VWAP), và ghi kết quả vào HBase (serving) và lưu parquet vào HDFS để làm dataset cho batch.
4. Spark Batch chạy nightly/weekly để rebuild historical views, compute features, huấn luyện/score models, và ghi artifacts (models) vào model store (e.g., MLflow).
5. API service đọc từ HBase + Redis cache để trả dữ liệu cho dashboard/clients.

## 7. Schema & Contract

- Thiết kế schema events JSON tối thiểu: {"timestamp": 1670000000, "source": "binance", "type": "trade", "symbol": "BTCUSDT", "price": 47000.12, "size": 0.001, "metadata": {...}}
- Sử dụng Avro/Protobuf schemas phối hợp với Schema Registry (Confluent) để validate và versioning.

## 8. Bảo mật & Compliance

- TLS mọi kết nối: Kafka (SSL), REST endpoints HTTPS.
- Encryption at-rest: HDFS + object storage hỗ trợ server-side encryption.
- RBAC: Kubernetes RBAC cho deployment; service accounts cho các job.
- Secrets: Vault / Kubernetes Secrets (sealed) để lưu API keys, DB creds.
- Audit: lưu audit logs (access, changes) vào ELK.

## 9. Observability

- Metrics: exporters cho Kafka, Spark, HBase, collectors (Prometheus).
- Dashboards: Grafana templates cho throughput, consumer-lag, job-latency, error-rate.
- Logging: ứng dụng log structured JSON, shipper (Filebeat) về Elasticsearch.
- Tracing: so với phân tán pipeline, tích hợp Jaeger để track requests/alerts.

## 10. Triển khai & Scaling

- Kafka: scale bằng partitions + brokers. Partition key = symbol (hash) → giữ order per-symbol.
- Spark: chạy trên YARN/Kubernetes, auto-scale worker nodes cho batch/streaming.
- HBase: thêm regionservers; thiết kế region split theo time windows/symbol prefix để tránh hotspot.
- K8s: Horizontal Pod Autoscaler cho microservices; use node pools cho workloads khác nhau (IO-heavy vs CPU-heavy).

## 11. Lộ trình triển khai (Roadmap gói nhanh 8 tuần)

- Tuần 1-2: Thiết lập infra dev: Kafka (local/docker-compose or k8s), minio/HDFS dev, k8s cluster (local/managed), skeleton collectors.
- Tuần 3-4: Triển khai Spark Structured Streaming dev jobs; publish basic metrics; đơn vị test end-to-end (producer -> streaming -> HBase -> API).
- Tuần 5-6: Hoàn thiện batch pipelines, Airflow DAGs, model training pipeline, và MLflow integration.
- Tuần 7: Load testing, scaling tests, SLO tuning.
- Tuần 8: Harden security, backups, runbook, handover docs.

## 12. Rủi ro & biện pháp giảm thiểu

- Hotspot partitions (Kafka/HBase): thiết kế partition key hợp lý, sharding helper.
- Tổn thất dữ liệu producer failure: enable producer acks, idempotent producers, DLQ topic.
- Stream job state explosion: use TTL for state stores, checkpointing, compacted topics for state snapshots.

## 13. Chi phí ước tính (sơ bộ)

- Dev (Poc): nhỏ, chạy trên 3–5 nodes (k8s) hoặc local docker.
- Prod: tùy quy mô (ví dụ: 3 Kafka brokers + 3 ZK + spark workers(3–6) + storage): chi phí hạ tầng cloud ước tính vài trăm tới vài nghìn USD/tháng tùy yêu cầu SLA và retention.

## 14. Kết luận & bước tiếp theo

- Kiến trúc Lambda (batch + streaming) phù hợp khi cần audit và khả năng replay/historical recompute cho mục tiêu nghiên cứu và báo cáo.
- Bước tiếp theo (gấp):
  - Xác nhận data sources và retention policy (raw vs curated).
  - Thiết lập environment dev (docker-compose hoặc k8s minikube) để chạy POC minimal.
  - Viết 3 DAG Airflow: ingestion-checkpoint, daily-batch, ml-train.

---

_File này là bản đề xuất kiến trúc tổng quát, có thể tuỳ chỉnh theo hạn mức hạ tầng và yêu cầu SLA cụ thể._
