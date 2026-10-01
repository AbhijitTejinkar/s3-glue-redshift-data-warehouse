\# S3 → AWS Glue → Amazon Redshift Data Warehouse Pipeline



\## Project Overview

Automated ETL pipeline that ingests raw data from Amazon S3, transforms it using PySpark in AWS Glue, and loads curated data into Amazon Redshift for analytics and BI reporting.



\## Architecture



S3 (Raw) → AWS Glue (PySpark) → S3 (Staging Parquet) → Redshift COPY → Redshift Tables → Power BI



\## Tech Stack

\- Amazon S3

\- AWS Glue (PySpark)

\- Amazon Redshift

\- AWS Lambda

\- AWS IAM

\- Amazon CloudWatch

\- Amazon QuickSight  Power BI



\## Key Features

\- Automated S3-to-Redshift ingestion using Glue and COPY command

\- PySpark transformations for cleansing, standardization, and enrichment

\- Incremental processing using Glue Job Bookmarks

\- Redshift distribution and sort key optimization

\- CloudWatch monitoring and alerting

\- IAM-based least-privilege security



\## Results

\- Reduced batch window by 50%

\- Cut job runtime by 60%

\- Lowered AWS compute costs significantly

\- Enabled daily BI refresh for retail analytics teams



\## How to Run

1\. Upload raw files to `s3raw-bucket`

2\. Glue job reads and transforms data

3\. Writes Parquet to `s3staging-bucket`

4\. Redshift COPY loads data into target tables

5\. Query via Redshift or connect to Power BI



\## Author

Abhijit Tejinkar — Senior Data Engineer

