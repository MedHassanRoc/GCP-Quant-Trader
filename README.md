# QuantEdge — End-to-End Quant Trading Analytics Platform

QuantEdge is a cloud-native trading analytics project that demonstrates the design and implementation of a full **data and machine learning pipeline** for quantitative trading using modern GCP technologies.

The project covers the complete workflow: **data ingestion → transformation → feature engineering → model training → orchestration → deployment → monitoring → visualization**, all within a production-ready cloud framework.

---

## 🧭 Description

QuantEdge showcases how to build a scalable, maintainable, and automated analytics system for financial time-series modeling on Google Cloud.  
It simulates how a real trading research team would collect market data, engineer predictive features, train ML models, deploy inference APIs, and monitor results.

The platform combines best practices from **data engineering**, **analytics engineering**, and **MLOps**, leveraging **BigQuery** and **Vertex AI** within one cohesive GCP environment.

---

## ⚙️ Main Components

- **Infrastructure (Terraform)** – provisions GCP resources (GCS buckets, BigQuery datasets, service accounts, Cloud Run service, Composer environment).  
- **Data Ingestion (Python)** – fetches OHLCV market data (e.g., Binance BTCUSDT) and stores it in Cloud Storage.  
- **Data Warehouse (BigQuery)** – serves as the central analytical layer with dbt-managed models.  
- **Transformation (dbt)** – modular SQL pipelines (raw → int → mart) with tests, documentation, and feature generation.  
- **Machine Learning (Python / Vertex AI)** – trains, evaluates, and deploys predictive models for asset direction or return forecasting using managed Vertex AI training and endpoints.  
- **Automation & Orchestration (Airflow / Cloud Composer)** – orchestrates ingestion, dbt transformations, model training, and Vertex AI deployments.  
- **Serving (FastAPI on Cloud Run)** – lightweight REST API exposing endpoints (`/health`, `/predict`) for real-time inference.  
- **Monitoring (BigQuery + Cloud Monitoring)** – collects pipeline metrics, model drift, and API performance logs.  
- **Visualization (Looker Studio)** – interactive dashboards for performance, feature metrics, and trading KPIs using LookMl.  
- **CI/CD (GitHub Actions)** – automates testing, dbt and Terraform validation, container build, and Cloud Run deployment.


---

## 🧩 Example Flow

1. Fetch raw OHLCV data from Binance and store as Parquet in GCS.  
2. Load and transform data into BigQuery (raw → int → mart) via dbt.  
3. Train and evaluate ML models in Vertex AI using engineered features.  
4. Deploy trained models to Vertex AI managed endpoints.  
5. Orchestrate the full pipeline with Airflow (Cloud Composer).  
6. Serve predictions in real-time with FastAPI on Cloud Run.  
7. Visualize metrics and trading performance in Looker Studio.

---

## 🧠 Focus Areas

- End-to-End GCP Data & ML Architecture  
- Data Modeling and Testing with dbt + BigQuery  
- Scalable ML Training and Serving via Vertex AI  
- Workflow Orchestration with Airflow (Cloud Composer)  
- CI/CD and Infrastructure as Code (Terraform + GitHub Actions)  
- Observability and Visualization with Looker Studio  

---

**Author:** Mohamed Hassan Oukhouya  
_Analytics Engineer & Quant Enthusiast_
