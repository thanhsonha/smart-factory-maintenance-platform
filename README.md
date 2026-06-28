# Smart Factory Maintenance Analytics Platform

## Overview

An end-to-end industrial analytics platform designed to monitor machine health, process sensor data, and generate predictive maintenance insights.

This project implements a Medallion Architecture (Bronze → Silver → Gold) using Spark, Kafka, Airflow, and Power BI to simulate a modern smart manufacturing analytics environment.

---

## Architecture

```text
AI4I 2020 Dataset
        │
        ▼
Kafka Producer
        │
        ▼
Spark Streaming
        │
        ▼
Bronze Layer
(raw sensor events)
        │
        ▼
Silver Layer
(cleaned & validated data)
        │
        ▼
Gold Layer
(KPIs & reliability metrics)
        │
        ▼
Airflow Orchestration
        │
        ▼
Power BI Dashboard
```

---

## Tech Stack

| Category           | Technologies           |
| ------------------ | ---------------------- |
| Programming        | Python                 |
| Streaming          | Kafka                  |
| Processing         | Apache Spark           |
| Orchestration      | Airflow                |
| Storage            | Parquet                |
| Analytics          | Power BI               |
| Containerization   | Docker                 |
| Data Modeling      | Medallion Architecture |
| Quality Monitoring | Spark Data Validation  |

---

## Dataset

Dataset used:

AI4I 2020 Predictive Maintenance Dataset

Contains:

* machine type
* air temperature
* process temperature
* rotational speed
* torque
* tool wear
* machine failure indicators

Failure labels include:

* Tool Wear Failure
* Heat Dissipation Failure
* Power Failure
* Overstrain Failure
* Random Failure

---

## Project Structure

```text
smart-factory-maintenance-platform/

producer/
consumer/

spark/
jobs/

src/
jobs/

airflow/
dags/

dashboard_exports/

docs/
images/

data/
raw/
lake/

README.md
```

---

## Medallion Architecture

### Bronze Layer

Raw sensor events ingested from Kafka.

Stored as parquet files.

Contains:

* event_time
* machine_id
* product_id
* torque
* temperature
* failure indicators

---

### Silver Layer

Validated and transformed records.

Processes include:

* schema enforcement
* duplicate removal
* feature engineering
* machine health classification

Example attributes:

* machine_health_status
* torque_nm
* tool_wear_min
* machine_failure

---

### Gold Layer

Aggregated KPIs for analytics.

Metrics generated:

* failure_rate_pct
* total_failures
* power_failure
* overstrain_failure
* random_failure
* tool_wear_failures

Example results:

| Machine Type | Failure Rate |
| ------------ | ------------ |
| L            | 3.58%        |
| M            | 1.97%        |
| H            | 1.05%        |

---

## Data Quality Checks

Implemented automated validation checks.

Metrics:

```text
Total Records : 1597
Valid Records : 1597
Duplicate Records : 0
Invalid Records : 0

Status : PASS
```

---

## Airflow Pipeline

Pipeline orchestration using Airflow.

DAG:

```text
bronze_to_silver
        ↓
silver_to_gold
        ↓
data_quality_check
```

Screenshot:

```text
docs/images/airflow_pipeline.png
```

---

## Dashboard

Power BI dashboard includes:

* Failure Rate by Machine Type
* Failure Distribution
* Reliability KPIs
* Data Quality Metrics
* Maintenance Monitoring

Screenshot:

```text
docs/images/powerbi_dashboard.png
```

---

## Running the Project

Clone repository

```bash
git clone https://github.com/thanhsonha/smart-factory-maintenance-platform.git
cd smart-factory-maintenance-platform
```

Install dependencies

```bash
pip install -r requirements.txt
```

Start services

```bash
docker compose -f docker-compose-airflow.yml up -d
```

Access Airflow

```text
http://localhost:8080
```

---

## Future Improvements

* ML-based predictive maintenance models
* Real-time anomaly detection
* dbt transformations
* Great Expectations integration
* Cloud deployment on AWS/GCP
* Grafana monitoring

---

## Author

Thanh Son Ha

MS Business Analytics

Cal Poly Pomona

Focus Areas:

* Data Engineering
* Manufacturing Analytics
* Predictive Maintenance
* Machine Learning
* Data Platforms
  

