# AWS SageMaker Workshop: QuickBite Sales Data Pipeline

This repository houses the QuickBite Sales (QBS) Data Pipeline, built entirely within AWS SageMaker Unified Studio (SMUS). Designed specifically as a workshop prototype, it showcases a complete, end-to-end medallion data pipeline—transforming raw sales data into actionable, Gold-layer business metrics.

## Overview

The pipeline transitions raw CSV sales records from an S3 bronze bucket into a SageMaker Unified Studio project workspace. It uses a Visual ETL job to conduct data quality checks and split the data into dimension and fact tables in Parquet format, saved in a Silver conformed bucket. A secondary script-based Glue job saving it to a Gold curated bucket.

SMUS Platform provides:
*   A governed, single-project workspace using SageMaker Unified Studio.
*   Data Catalog integration to infer schemas and save metadata.
*   Visual ETL Glue jobs for splitting fact and dimension tables.
*   Data Quality Checks (DQDL) that route failed rows to a quarantine bucket.
*   Script-based Glue jobs for transforming Silver data into aggregated Gold-layer business metrics.

## Repository Layout

```text
/docs
    Sagemaker Project Brief and Output.pdf            Sagemaker Project Brief and Output
/notebooks
    business question 1 - branch performance.ipynb    EDA for volume vs. price effect
    business question 2.ipynb                         EDA for menu engineering
    business question 3 - channel behaviour.ipynb     EDA for channel mix
    sales_raw eda.ipynb                               Initial data profiling and exploration
/scripts
    /qbs-bronze-to-silver
        qbs-bronze-to-silver.json                     Job configuration file
        qbs-bronze-to-silver.py                       Generated Python script
        qbs-bronze-to-silver.vetl                     SMUS Visual ETL job file
    /qbs-silver-to-gold
        qbs-silver-to-gold.json                       Job configuration file
        qbs-silver-to-gold.py                         Data processing job script
```

## Data Flow & Architecture

```text
S3 01_bronze/sales.csv
                |
                v
Add Data Source: Data Catalog (Infers schema)
                |
                v
SMUS Visual ETL Job (qbs-bronze-to-silver)
    - Evaluate Data Quality (dqdl-ruleset)
    - Split fact & dim tables
                |
                +--> Failed rows route to: S3 04_quarantine/
                |
                v
S3 bucket (conformed) 02_silver/
                |
                +--> qbs_smus_silver.dim_branch
                +--> qbs_smus_silver.dim_product
                +--> qbs_smus_silver.fact_sales
                |
                v
SMUS Code Job (qbs-silver-to-gold)
    - Generate aggregated KPIs
                |
                v
S3 bucket (curated) 03_gold/
                |
                +--> qbs_smus_gold.branch_performance
                +--> qbs_smus_gold.menu_quadrant
                +--> qbs_smus_gold.menu_rank_volatility
                +--> qbs_smus_gold.channel_mix
                +--> qbs_smus_gold.channel_growth_correlation
                |
                v
SageMaker Catalog (Publish curated metadata)
                |
                +--> AWS Athena (Ad-hoc SQL)
                +--> AWS QuickSight (Dashboards)
```

## Getting Started in SMUS

This repository provides template files. Because AWS environments are unique, you must update the provided scripts and configuration files with your specific account details before running the jobs.

1. Ensure your S3 bucket prefixes (`01_bronze/`, `02_silver/`, `03_gold/`, `04_quarantine/`) are provisioned.
2. Upload your raw source file (`Sales-Data-Analysis.csv`) to the `01_bronze/` prefix and add the Data Source to the Data Catalog to infer the schema for the `sales_raw` table.
3. Navigate to the `/scripts` directory in your cloned repository, open the `.json`, `.py`, and `.vetl` files, and replace the environment-specific placeholders (e.g., `<YOUR_ACCOUNT_ID>`, `<YOUR_BUCKET_NAME>`) with the actual values from your AWS deployment.
4. In your SMUS environment, create a new Visual ETL workflow, import the modified `qbs-bronze-to-silver.vetl` file, verify the S3 paths and Data Catalog nodes, and run the job.
5. Create a new Code Job in SMUS, import the modified `qbs-silver-to-gold.py` script, and execute it to generate the Gold KPIs.
6. Once the jobs complete successfully, register the Silver and Gold tables to the SageMaker Catalog.
7. Launch a JupyterLab notebook environment attached to the project's compute and upload the `.ipynb` files from the `/notebooks` folder to perform Exploratory Data Analysis (EDA) on the business questions.