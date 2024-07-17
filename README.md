# Databricks ETL Pipeline for Traffic and Roads Data with Unity Catalog

## Project Overview

This project involves building an ETL (Extract, Transform, Load) pipeline using Databricks and Azure Data Lake Storage Gen 2 (ADLS Gen 2) to process and transform traffic and roads data. The project leverages Databricks for data processing, Unity Catalog for data governance, and Azure DevOps for CI/CD.

## Architecture

The project is designed with a multi-layer architecture to handle raw data ingestion, transformation, and storage. The layers include:

1. **Landing Zone:** Raw CSV files from ADLS Gen 2 are ingested into the landing zone.
2. **Bronze Layer:** Raw data is cleaned and transformed into a more structured format.
3. **Silver Layer:** Additional transformations are applied, including aggregations and time-based transformations.
4. **Gold Layer:** Final transformations for quantifiable metrics and load time tracking.

![Architecture](https://github.com/DivineSamOfficial/Traffic-and-Roads-Databricks-ETL-Pipeline-with-Unity-Catalog/blob/main/Assets/SysArch.jpg)

## Visulazationn with Power BI
![Report]()

## Databricks ETL Flow
![ETL Flow](path-to-image)


## Key Features

### Dynamic Schema Creation

- Utilized Databricks widgets for flexible schema definitions.
- Allows notebooks to run seamlessly with dynamic configurations.

### Data Ingestion

- Implemented Spark Streaming with batch processing (availableNow trigger).
- Used Autoloader for incremental file loading as they arrive in ADLS.
- Monitored and audited data with CurrentTime stamp for precise tracking.

### Data Transformation

- **Bronze Transformations:**
  - Performed initial data cleaning, handling nulls, and removing duplicates.
  - Applied basic transformations to structure the data.
- **Silver Transformations:**
  - Parameterized notebooks tailored for the development environment.
  - Applied advanced PySpark transformations, including time extraction, column aggregations, and conventional column name cleaning.
- **Gold Transformations:**
  - Enhanced data with quantifiable columns and load time tracking.
  - Integrated with Unity Catalog for robust data governance and centralized user management.

### Automation and Orchestration

- **Databricks Workflows:** Automated ETL processes with triggers for new file arrivals in ADLS, ensuring timely and accurate data processing.
- **CI/CD with Azure DevOps Git and Unity Catalog:** Seamlessly integrated Databricks with Azure DevOps for continuous integration and deployment, leveraging Azure Active Directory for secure linkage.

## Setup and Configuration

### Prerequisites

- **Azure Subscription:** Ensure you have an active Azure subscription.
- **Databricks Workspace:** Create a Databricks workspace in your Azure subscription.
- **ADLS Gen 2:** Set up an ADLS Gen 2 account and create the necessary containers for raw and processed data.
- **Azure DevOps:** Set up an Azure DevOps project and repository.
- **Unity Catalog:** Configure Unity Catalog for data governance.

