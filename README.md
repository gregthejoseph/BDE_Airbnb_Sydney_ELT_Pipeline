# 🏡 Airbnb Sydney ELT Pipeline

## 📖 Project Overview
This project demonstrates a **production-ready ELT (Extract, Load, Transform)** pipeline for analyzing **Airbnb listings and Census data** for Sydney.  
The solution was deployed using **Google Cloud Composer (Airflow)** for orchestration and **Google Cloud SQL (PostgreSQL)** as the data warehouse.  
All transformations were built in **dbt Cloud**, and final analytical layers were visualized and validated in **DBeaver**.

---

## ☁️ Architecture Overview
| Layer | Purpose | Platform / Tool |
|-------|----------|----------------|
| **Bronze** | Raw data ingestion from CSV files | Airflow on GCP Composer |
| **Silver** | Data cleaning, transformation, and enrichment | dbt Cloud |
| **Gold** | Analytical fact and dimension tables for insights | Cloud SQL (PostgreSQL), queried via DBeaver |

---

## ⚙️ Tech Stack
- **Google Cloud Composer (Airflow)** – Workflow orchestration  
- **Google Cloud SQL (PostgreSQL)** – Central data warehouse  
- **dbt Cloud** – Data modeling and transformation  
- **DBeaver** – SQL exploration and visualization  
- **Python / SQL / Jinja** – Scripting and transformations  

---

## 🚀 Workflow Summary
1. Raw Airbnb and Census CSV files are ingested into **Cloud SQL (Bronze layer)** using **Airflow DAGs** in **GCP Composer**.  
2. **dbt Cloud** transforms the data into **Silver** and **Gold** layers following the Medallion Architecture.  
3. Final analytical tables are stored in **Cloud SQL** and accessed through **DBeaver** for insights and reporting.  

---

## 📊 Key Outcomes
- Built a scalable, modular ELT pipeline entirely in the cloud.  
- Achieved clean, query-ready data layers with dbt transformations.  
- Enabled seamless orchestration, lineage tracking, and visualization.  
- Delivered actionable insights on Airbnb pricing, reviews, and neighbourhood performance across Sydney.

---

## 👨‍💻 Author
**Gregory Joseph**  
Master of Data Science & Innovation, UTS Sydney  
[LinkedIn](www.linkedin.com/in/gregoryjoseph595) • [GitHub](https://github.com/gregthejoseph)
