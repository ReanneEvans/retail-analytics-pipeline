# 🛒 Retail Analytics Pipeline (Python + MySQL + Power BI)

## 📌 Project Overview
This project demonstrates a **real-world analytics pipeline** where raw CSV files were cleaned and transformed in **Python**, structured into a **MySQL database**, and visualised through an **interactive Power BI dashboard**.

**Business Problem**  
Retail managers often lack a consolidated view of performance when data is split across sales, customers, products, and stores. They need integrated reporting to answer:
- Which products and categories drive the most revenue?
- Who are the most valuable customers?
- How do sales and profit margins trend over time?

**Solution**  
I built a full pipeline that:
1. Cleaned and standardized raw CSVs in **Python (pandas)**.  
2. Loaded the data into **MySQL** and built KPI queries/views.  
3. Connected Power BI to MySQL for a **dynamic executive dashboard**.  

---

## 🛠️ Tools & Technologies
- **Python** → pandas, SQLAlchemy (data cleaning & export)  
- **MySQL** → schema design, KPI queries, analytical views  
- **Power BI** → dashboards, DAX measures, business insights  

---

## ⚙️ Workflow

### 1. Data Cleaning (Python)
- Cleaned 5 CSV datasets: **Sales, Products, Customers, Stores, Exchange Rates**.
- Standardized column names, parsed dates, validated missing/duplicate values.
- Created new features:
  - `delivery_time` (days between order and delivery)  
  - `delivered` flag  
  - Cleaned unit prices/costs into numeric  

👉 [See Python Script](./python/cleaning_pipeline.py)

---

### 2. Data Storage & Analysis (MySQL)
- Exported cleaned data into **MySQL** using SQLAlchemy.  
- Created **views** for KPIs and advanced analysis, e.g.:  
  - `v_top_customers` → Top 10 customers by revenue  
  - `v_category_yoy_growth` → Category revenue with YoY growth %  
  - `v_customer_cohort_retention` → Cohort retention by month  
  - `v_store_performance` → Store revenue & productivity (per m²)  

👉 [See SQL Scripts](./sql/analysis_views.sql)

---

### 3. Dashboard (Power BI)
Built an interactive **executive dashboard** connected directly to MySQL.  
- **KPI cards** →  Repeat %, Avg Delivery Time  
- **Trend charts** → Monthly revenue, MoM growth, moving averages  
- **Category growth** → Revenue contribution & YoY growth  
- **Customer insights** → Repeat vs New (donut), cohort retention matrix  
- **Filters & drilldowns** → by year, customer, product category


👉 [**View Interactive Dashboard**](https://app.powerbi.com/view?r=eyJrIjoiNjBhNmVkZTAtMTdiYi00MTYzLWJhMDctZmRkZjc0YTg4MjE5IiwidCI6Ijc0M2ZkMmZhLTA1NTUtNGFhYy1iMjFjLTMyMWUzYzIwMWRiMyJ9)
---

## 💡 Business Impact
- **Unified reporting** → leadership can see all KPIs in one place  
- **Growth visibility** → highlighted strong categories & underperformers  
- **Customer insights** → identified top customers & repeat purchase behavior  
- **Operational visibility** → measured delivery times & store productivity  

