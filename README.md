# Data Engineering Project: ETL Pipeline with Airflow & PySpark

This project demonstrates a complete ETL pipeline that:

* Extracts data from multiple sources using Apache Airflow

* Cleans and transforms data using PySpark

* Stores the cleaned data in a MySQL database

# Prerequisites
* Docker
* Docker Compose

# Data Sources & API Keys
This project uses external APIs to extract real-time data:
Source	Description	Notes
* EIA	US Energy Information Administration – Fuel Prices	Free with API key (sign-up required)
* ExchangeRate Host	Free foreign exchange rates API	Free with API key (sign-up required)
* Stormglass	Marine weather data (wind, waves, etc.)	Limited to 10 API calls/day. API key required

# Setup Instructions
1. Clone the repository
* git clone https://github.com/RanyaHasnaoui/Shipping_costs.git
* cd Shipping_costs

2. Create your .env file
* cp .env.example .env
* Edit .env and set your credentials:

Example:

MYSQL_HOST=mysql  
MYSQL_PORT=3306  
MYSQL_DB=shipping_costs  
MYSQL_USER=user  
MYSQL_PASSWORD=password  

# Run the containers
docker-compose up --build  
This will spin up:  

* Airflow Scheduler & Webserver

* Spark container

* MySQL container

(Optional) Jupyter or Spark UI container

# Running the Pipeline
The ETL pipeline is triggered by Airflow DAGs

The transformation logic is handled in data_cleaning.py using PySpark

Cleaned data is written back to MySQL

# Security Notice
.env is ignored via .gitignore

Do not store credentials directly in your Python or DAG scripts

Always use environment variables or secrets

# Useful Commands :
* List Docker containers  
docker ps

* Open a shell in Spark container  
docker exec -it spark-container-name bash

* Access Airflow web UI (usually at http://localhost:8080)

* View logs for Spark  
docker logs spark-container-name  

# Author
Ranya Hasnaoui  
LinkedIn [https://www.linkedin.com/in/rania-hasnaoui-365716213/]  
GitHub [https://github.com/RanyaHasnaoui]  
