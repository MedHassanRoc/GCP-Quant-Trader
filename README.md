# QuantEdge — End-to-End Quant Trading Analytics Platform

QuantEdge is a cloud-native trading analytics project that demonstrates the design and implementation of a full **data and machine learning pipeline** for quantitative trading using modern GCP technologies.

The project covers the complete workflow: **data ingestion → transformation → model training → orchestration → deployment → monitoring → visualization**, all within a production-ready cloud framework.

---

## 🧭 Description

QuantEdge showcases how to build a scalable, maintainable, and automated analytics system for financial time-series modeling on Google Cloud.  
It simulates how a real trading research team would collect market data, engineer predictive features, train ML models, deploy an inference API, and track performance.

The platform combines best practices from **data engineering**, **analytics engineering**, and **MLOps** to deliver an end-to-end quant solution.

---

## ⚙️ Main Components

- **Infrastructure (Terraform)** – creates GCP resources (GCS bucket, BigQuery dataset, service accounts, Cloud Run service).
- **Data Ingestion (Python)** – fetches OHLCV market data from public APIs and stores it in Cloud Storage.
- **Data Warehouse (BigQuery)** – central layer for transformation and modeling using SQL and dbt.
- **Transformation (dbt)** – modular data models (bronze → silver → gold) with tests, documentation, and feature generation.
- **Machine Learning (Python / Vertex AI)** – trains predictive models for asset direction or return forecasting.
- **Orchestration (Prefect / Cloud Scheduler)** – automates ingestion, transformations, and training workflows.
- **Serving (FastAPI on Cloud Run)** – exposes REST endpoints (`/health`, `/predict`) for real-time inference.
- **Monitoring (BigQuery + Logging)** – stores pipeline metrics, run heartbeats, and model evaluation results.
- **Visualization (Looker Studio)** – interactive dashboards for performance, feature metrics, and strategy results.
- **CI/CD (GitHub Actions)** – automated testing, dbt validation, Terraform validation, and API deployment.

---

## 🧩 Example Flow

1. Fetch raw OHLCV data and save to Cloud Storage.  
2. Load data into BigQuery (bronze layer) via dbt.  
3. Clean and feature-engineer data (silver, gold layers).  
4. Train ML models on engineered features.  
5. Store predictions and metrics back in BigQuery.  
6. Serve predictions through FastAPI on Cloud Run.  
7. Visualize accuracy and trading metrics in Looker Studio.

---

## 🧠 Focus Areas

- Cloud Data Engineering and Architecture on GCP  
- Analytics Engineering with dbt and BigQuery  
- Applied Machine Learning and Model Lifecycle  
- MLOps and Orchestration Automation  
- Reproducibility, CI/CD, and Documentation  
- Real-time API deployment and visualization

---

**Author:** Mohamed Hassan Oukhouya  
Analytics Engineer & Quant Enthusiast
