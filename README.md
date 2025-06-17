## Spotify-ETL-Pipeline

### 📌 Project Overview

This data engineering project focuses on extracting, ingesting, transforming, and visualizing weekly updates from the **Spotify Top Global Songs** playlist using a cloud-native ETL pipeline. The workflow simulates a modern data pipeline using **AWS services (CloudWatch, Lambda, S3, Glue)**, **Snowflake**, and **Power BI** to enable real-time trend tracking and analytics. The pipeline is designed to automatically fetch the latest top songs data weekly, transform it into structured formats, and deliver insights through a Power BI dashboard.

---

### 🛠️ Tools & Technologies Used

<p align="left">
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/> 
<img src="https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white" alt="AWS Lambda"/> 
<img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3"/>
<img src="https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/>
<img src="https://img.shields.io/badge/AWS%20Glue-6B47DC?style=for-the-badge&logo=amazonaws&logoColor=white" alt="AWS Glue"/>
<img src="https://img.shields.io/badge/Snowflake-56B9EB?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake"/> 
<img src="https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI"/> 
</p>

---

### 📊 Architecture

![Project Architecture](project_architecture.png)

---

### 📑 Data Source

The project uses the [Spotify Web API](https://developer.spotify.com/documentation/web-api/) to access the **Top Songs Global** playlist, refreshed weekly. The data includes:


- **Album Info**: Album name, release date, total tracks.
- **Artist Details**: Artist name, ID, genres.
- **Song Metadata**: Song name, duration, popularity, etc.
- **Timestamps**: Ingestion date for weekly tracking.

---

### 🚀 Engineering Process

1. **Data Extraction**:
- **AWS Lambda** is scheduled via **CloudWatch** to run weekly.
- The Lambda function fetches Top Songs Global data from the Spotify API.
- Raw JSON data is saved into the **S3 RAW zone**, partitioned by date.

2. **Data Transformation**:
- **AWS Glue** job is triggered by the Lambda function on an S3 event when new raw data is added.
- PySpark processes raw S3 data into structured datasets: `albums`, `artists`, `songs`.
- Transformed data is stored in the **S3 Transformed zone**.

3. **Data Loading**:
- **Snowflake Database** is created to serve as the analytics warehouse.
- A **Storage Integration (`s3_init`)** securely connects Snowflake to the S3 Transformed zone via an external stage.
- **Snowpipe** is configured with `AUTO_INGEST` to load new data into `albums`, `artists`, and `songs` tables as soon as it's available.

4. **Data Serving**:
- **Power BI** connects directly to Snowflake.
- Interactive dashboards track weekly song popularity and artist trends.

---

### 🎵 Top Songs Global Analytics

![Power BI Dashboard](visualization/powerbi.png)
<!-- Replace with your actual Power BI dashboard screenshot path -->

---

