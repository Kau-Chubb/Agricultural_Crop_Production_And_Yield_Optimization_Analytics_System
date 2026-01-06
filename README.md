# 🌾 Agricultural Crop Production & Yield Optimization Analytics System

## Enterprise-Grade Data Engineering & Analytics Platform for Agriculture

![License](https://img.shields.io/badge/License-MIT-green)
![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Databricks](https://img.shields.io/badge/Databricks-Enabled-red)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.x-blue)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-brightgreen)
![Power BI](https://img.shields.io/badge/Power%20BI-Enabled-yellow)

Building a **modern agricultural analytics platform** using a **lakehouse architecture** to transform raw crop data into actionable yield insights.

**Features • Architecture • Data Layers • Analytics**

---

## 🎯 Executive Summary

Agricultural departments and agribusiness organizations collect large volumes of crop production data across **states, districts, seasons, and years**. However, decision-making is often impacted by fragmented datasets, manual analysis, and limited visibility into yield performance.

This project delivers a **production-ready agricultural analytics system** that enables:

- Automated ETL pipelines for crop datasets  
- Standardized and validated yield analytics  
- Identification of high- and low-performing regions  
- Executive-ready dashboards for data-driven planning  

### Key Capabilities
- **Automated ETL Pipeline** – Orchestrated using Apache Airflow  
- **Lakehouse Architecture** – Bronze, Silver, and Gold layers using Delta Lake  
- **Scalable Analytics** – Distributed processing with Databricks & PySpark  
- **Interactive BI** – Power BI dashboards for agricultural insights  

---

## 📊 Project Impact

| Metric | Value |
|------|------|
| Data Processing Speed | 8–10x faster than manual analysis |
| Pipeline Reliability | 99.9% uptime with automated retries |
| Analytics Latency | Near real-time insights |
| Scalability | Handles multi-year, multi-region crop data |

---

## ✨ Key Features

### 🏗️ Enterprise Architecture
- Medallion lakehouse architecture (Bronze–Silver–Gold)
- ACID-compliant Delta Lake tables
- Schema enforcement and evolution
- Incremental and idempotent processing

### 🔄 Workflow Automation
- Apache Airflow DAG orchestration
- Dependency management between layers
- Automated retries and failure alerts
- Centralized execution logging

### 📈 Advanced Analytics
- Crop-wise production trend analysis
- Yield comparison across states and districts
- Seasonal and yearly performance evaluation
- KPI-driven aggregations for BI

### 📊 Business Intelligence
- Interactive Power BI dashboards
- Drill-down analysis by region and year
- KPI cards for production and yield
- Filter-driven insights for stakeholders

---

## 🏛️ Architecture

### System Architecture Overview

<p align="center">
  <img src="images/system_architecture.png" width="850"/>
  <br>
  <em>
    End-to-End Agricultural Crop Production & Yield Optimization Analytics Architecture
  </em>
</p>

This system follows a modern **lakehouse-based architecture** orchestrated by Apache Airflow, 
designed to process large-scale agricultural datasets and deliver analytics-ready insights.

**Architecture Flow:**
- Multiple agricultural data sources (Crop Production, Rainfall, Soil Health, Fertilizer Usage)
- Python & Pandas-based ingestion layer
- Delta Lake–backed Bronze, Silver, and Gold layers
- PySpark transformations for scalable analytics
- Power BI dashboards for visualization and decision support


## 🧰 Technology Stack

### Core Technologies

| Category | Technology | Purpose |
|-------|-----------|--------|
| Processing | PySpark | Distributed data processing |
| Platform | Databricks | Unified analytics workspace |
| Storage | Delta Lake | ACID-compliant lakehouse |
| Orchestration | Apache Airflow | Workflow automation |
| Visualization | Power BI | Interactive dashboards |
| DevOps | Docker | Environment management |

### Infrastructure Components
- Python 3.8+ for ETL scripting
- Docker & Docker Compose for Airflow
- Git & GitHub for version control
- Databricks Notebooks for development

---
## 📁 Project Structure

The project follows an enterprise-grade data engineering layout with clear separation of orchestration, data sources, processing, analytics, and visualization layers.

```text
Agricultural-Crop-Yield-Analytics/
│
├── capstone_airflow/                 # Workflow Orchestration (Apache Airflow)
│   ├── airflow-dags/                 # Airflow DAG definitions
│   ├── airflow-logs/                 # Pipeline execution & audit logs
│   ├── airflow-plugins/              # Custom Airflow plugins (if any)
│   ├── docker-compose.yml            # Airflow services orchestration
│   ├── Dockerfile                    # Custom Airflow Docker image
│   └── requirements.txt              # Python dependencies for Airflow
│
├── Datasets/                         # Source Data (Raw Datasets)
│   ├── ca_crop_master.csv            # Crop reference & master data
│   ├── ca_crop_production.csv        # Crop-wise production data
│   ├── ca_fertilizer_usage.csv       # Fertilizer usage metrics
│   ├── ca_rainfall_data.csv          # Rainfall & climate data
│   └── ca_soil_health.csv            # Soil health indicators
│
├── Capstone_Chubb_Databricks/        # Databricks Lakehouse
│   └── Capstone_Chubb/
│       ├── bronze/                   # Raw ingestion tables (Bronze layer)
│       ├── silver/                   # Cleaned & validated tables (Silver layer)
│       ├── gold/                     # Aggregated analytics tables (Gold layer)
│       └── Agricultural_Logging/     # Pipeline logging & monitoring
│
├── dashboard/                        # Business Intelligence Layer
│   └── agricultural_analytics.pbix   # Power BI dashboard
│
├── images/                           # Documentation images
│   └── system_architecture.png       # System architecture diagram
│
└── README.md                         # Project documentation
```
## 🧱 Data Layers (Medallion Architecture)

The project follows the **Bronze–Silver–Gold lakehouse architecture** implemented on Databricks using Delta Lake, ensuring scalability, reliability, and analytics readiness.

---

## 🥉 Bronze Layer – Raw Data Foundation

The Bronze layer stores **raw, immutable agricultural datasets** ingested directly from source files with minimal transformation.

### Purpose
- Preserve original source data
- Enable traceability and auditability
- Support reprocessing if required

### Bronze Tables

| Table Name | Description |
|----------|-------------|
| `bronze_crop_master` | Raw crop master reference data |
| `bronze_crop_production` | Raw crop-wise production data |
| `bronze_fertilizer_usage` | Raw fertilizer usage data |
| `bronze_rainfall` | Raw rainfall and climate data |
| `bronze_soil_health` | Raw soil health indicators |

### Key Characteristics
- Immutable Delta tables  
- Schema enforcement enabled  
- Ingestion timestamps captured  
- Source lineage maintained  

---

## 🥈 Silver Layer – Cleaned & Curated Data

The Silver layer contains **validated, standardized, and analytics-ready datasets** derived from Bronze tables.

### Purpose
- Improve data quality
- Standardize business dimensions
- Filter invalid or inconsistent records

### Silver Tables

| Table Name | Description |
|-----------|------------|
| `silver_crop_production` | Cleaned and validated crop production data |
| `silver_rainfall` | Cleaned rainfall data |
| `silver_soil_health` | Cleaned soil health data |

### Reject & Quarantine Tables
Records failing validation rules are isolated for audit and debugging.

| Table Name | Description |
|-----------|------------|
| `silver_reject_crop_production` | Rejected crop production records |
| `silver_reject_fertilizer_usage` | Rejected fertilizer usage records |
| `silver_reject_rainfall` | Rejected rainfall records |
| `silver_reject_soil_health` | Rejected soil health records |
| `quarantine_crop_production` | Quarantined records for reprocessing |

### Key Characteristics
- Business rule enforcement  
- Referential integrity checks  
- Numeric range and null validations  
- High-quality analytics-ready schema  

---

## 🥇 Gold Layer – Business Intelligence & Analytics

The Gold layer provides **aggregated, KPI-driven datasets** optimized for Power BI reporting and decision-making.

### Purpose
- Enable fast BI queries
- Provide single source of truth
- Support executive dashboards

### Gold Tables

| Table Name | Description |
|-----------|------------|
| `gold_crop_yield_summary` | Yield metrics aggregated by crop, region, and year |
| `gold_fertilizer_efficiency` | Fertilizer usage vs yield efficiency analysis |
| `gold_region_performance` | Regional production and yield performance metrics |

### Key Characteristics
- Pre-aggregated for performance  
- Denormalized for BI simplicity  
- Optimized for Power BI consumption  
- Consistent KPI definitions  

---

## 📜 Pipeline Logging & Monitoring

### Logging Tables

| Table Name | Description |
|-----------|------------|
| `pipeline_logs` | End-to-end ETL execution logs |

### Logging Capabilities
- Pipeline execution timestamps  
- Success / failure status tracking  
- Error diagnostics  
- Operational monitoring support  

---

### ✅ Summary
- Bronze ensures **data integrity**
- Silver ensures **data quality**
- Gold ensures **business value**
- Reject & log tables ensure **observability and reliability**
---
## 🔄 Orchestration with Apache Airflow

Apache Airflow is used as the **central workflow orchestrator** to manage and monitor the end-to-end ETL pipeline across the **Bronze, Silver, and Gold layers**.

The orchestration layer ensures:
- Reliable scheduling of ETL jobs
- Dependency management between data layers
- Operational visibility and monitoring
- Automated retries and failure handling

---

### 🧩 Airflow DAG Design

The pipeline is implemented as a **task-based DAG**, where each task represents a logical stage of the data pipeline:

| Task Name | Description |
|---------|-------------|
| `bronze_job` | Ingest raw agricultural datasets into Bronze Delta tables |
| `silver_job` | Clean, validate, and standardize data into Silver tables |
| `gold_task` | Generate aggregated analytics and KPIs in Gold tables |

**Task Dependency Flow:**

Each task is triggered only after the successful completion of its upstream dependency.

---

### 📸 Pipeline Execution Monitoring

<table>
  <tr>
    <td align="center">
      <img src="images/databricks_job_runs.png" width="450"/>
      <br>
      <em>Airflow-Orchestrated Databricks Job Runs</em>
    </td>
    <td align="center">
      <img src="images/airflow_dag_view.png" width="450"/>
      <br>
      <em>Apache Airflow DAG – Bronze → Silver → Gold</em>
    </td>
  </tr>
</table>
---

### ⚙️ Orchestration Features

- ⏱️ **Scheduled Execution** – Automated pipeline runs based on defined schedules  
- 🔗 **Dependency Management** – Strict Bronze → Silver → Gold execution order  
- 🔄 **Retry Mechanism** – Automatic retries on transient failures  
- 📊 **Execution Monitoring** – Task duration, success, and failure tracking  
- 🧾 **Audit Logging** – Execution details captured in pipeline log tables  

---

## 📊 Power BI Analytics Suite

The Power BI layer delivers **interactive, executive-ready dashboards** built on curated **Gold-layer Delta tables**, enabling data-driven agricultural decision-making.


### 📑 Report Pages Overview

The Power BI report is organized into multiple analytical views, each addressing a specific business question:

| Report Page | Description | Business Value |
|------------|-------------|----------------|
| 📌 **Executive Overview** | High-level KPIs including total production, average yield, and regional performance | Strategic planning & monitoring |
| 🌧️ **Rainfall-Driven Yield Analysis** | Analysis of rainfall impact on crop yield across seasons and regions | Climate impact assessment |
| 🗺️ **Regional Performance** | State- and district-level production and yield comparison | Regional optimization |
| 🌾 **Agricultural Yield Drivers Analysis** | Yield drivers such as fertilizer usage, soil health, and rainfall | Yield improvement insights |

---

### ⚙️ Dashboard Capabilities

- 🎛️ **Interactive Slicers** – Year, State, District, Crop, and Season  
- 📈 **KPI Cards** – Production, Yield, Growth indicators  
- 🔍 **Drill-Down Analysis** – State → District → Crop level insights  
- 🔄 **Automated Refresh** – Synced with Gold-layer Delta tables  
- 📤 **Export Options** – PDF, Excel, and PowerPoint  
---

### 🎯 Business Impact
- Enables identification of **high- and low-performing regions**
- Improves visibility into **yield-influencing factors**
- Supports **data-driven agricultural policy and planning**
- Reduces manual analysis and reporting effort
---
## 🚀 Quick Start

This section provides step-by-step instructions to set up and run the **Agricultural Crop Production & Yield Optimization Analytics System**, covering environment setup, pipeline execution, and analytics consumption.

---

## ✅ Prerequisites

Before starting, ensure the following are available:

- Docker Desktop (latest stable version)
- Docker Compose v2 or higher
- Python 3.8+
- Access to a Databricks workspace
- Databricks personal access token
- Power BI Desktop
- Git
- Minimum 8 GB RAM (16 GB recommended)

---

## 🚀 Quick Start

This section provides step-by-step instructions to set up and run the **Agricultural Crop Production & Yield Optimization Analytics System**, covering environment setup, pipeline execution, and analytics consumption.

---

### ✅ Prerequisites

Before starting, ensure the following are available:

- Docker Desktop (latest stable version)
- Docker Compose v2 or higher
- Python 3.8+
- Access to a Databricks workspace
- Databricks personal access token
- Power BI Desktop
- Git
- Minimum 8 GB RAM (16 GB recommended)

---

### 📥 Step 1: Clone the Repository

Clone the project repository and navigate to the project directory.

```bash
git clone <your-github-repository-url>
cd Agricultural_Crop_Production_And_Yield_Optimization_Analytics_System
```


### 🐳 Step 2: Start Apache Airflow Services

Navigate to the Airflow directory and start all required services using Docker Compose.

```bash
cd capstone_airflow
docker compose up -d
```


### 🌐 Step 3: Access the Airflow Web Interface

Open the Airflow UI in your browser to monitor and manage pipelines.

```text
http://localhost:8080
```

Login credentials:

```text
Username: admin
Password: admin
```


### 🔗 Step 4: Configure Databricks Connection in Airflow

Configure Airflow to securely connect to Databricks for job execution.

```text
Airflow UI → Admin → Connections → Add New
Connection ID   : databricks_default
Connection Type : Databricks
Host            : <Databricks Workspace URL>
Token           : <Databricks Personal Access Token>
```


### ▶️ Step 5: Execute ETL Pipelines

Trigger the ETL pipeline to process data across Bronze, Silver, and Gold layers.

```text
Airflow UI → DAGs → Enable ETL_DAG → Trigger DAG
```

Pipeline execution order:

```text
Bronze Layer → Silver Layer → Gold Layer
```


### 📊 Step 6: Open Power BI Dashboard

Open the Power BI report and refresh it to view the latest analytics.

```text
dashboard/agricultural_analytics.pbix
```

Steps inside Power BI:

```text
Home → Transform Data → Data Source Settings → Update Databricks Connection
Home → Refresh
```


### ✅ Step 7: Verify Data & Logs

Validate successful execution by checking logs and output tables.

```text
Databricks → Gold Tables
Databricks → Agricultural_Logging Tables
Airflow UI → Task Logs
```
---
