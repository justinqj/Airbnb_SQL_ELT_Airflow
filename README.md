# Data Processing and Analysis with Snowflake and Airflow

This project focuses on processing Airbnb data, enriching it with census data and local government area information, and constructing data marts for analysis. Data transformation and aggregation are accomplished through Snowflake SQL while orchestration is handled by Apache Airflow.

## 📋 Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [Data Processing Workflow](#data-processing-workflow)
- [Setting Up](#setting-up)
- [Usage](#usage)
- [Contributing](#contributing)

## 🌐 Overview

The goal is to build a pipeline that:
- Refreshes the staging tables from source data.
- Combines and enriches the source data.
- Creates data warehouse tables for detailed storage.
- Aggregates data into data marts for analysis.

## 📊 Data Sources

1. **Listing Data:** Details about Airbnb listings, including availability, pricing, and property type.
2. **Census Data:** Census data containing valuable metrics related to regions.
3. **Local Government Area (LGA) Data:** Information about local government areas including LGA names and corresponding suburbs.

## 🔁 Data Processing Workflow

1. **Data Refresh:** Raw data is refreshed in staging tables.
2. **Data Enrichment:** Data is combined, and LGA information is used to enrich the Airbnb listings.
3. **Data Warehouse Creation:** Detailed tables are created in the data warehouse to store enriched data.
4. **Data Mart Construction:** Data is aggregated to form analysis-friendly data marts.

## 🔧 Setting Up

### Prerequisites

- **Snowflake Account:** Ensure you have the necessary privileges to create and modify tables.
- **Apache Airflow:** Installation and configuration for orchestrating the workflow.

### Configuration

1. Set up your Snowflake connection in Airflow with the connection ID `snowflake_conn_id`.
2. Update the SQL queries in the DAG as necessary, especially if schema names or table structures change.

## 🚀 Usage

1. Activate the Airflow environment.
2. Start the Airflow web server and scheduler.
3. Access the Airflow UI and trigger the DAG for execution.
4. Monitor the progress and check logs for any issues.
