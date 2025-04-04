Developed a real-time ETL pipeline to collect currency exchange rate data from an API, stored it in AWS S3 (raw zone) with date-based partitioning, convertd it to Parquet using AWS Glue, and loaded it into Amazon Redshift (external tables) via dbt for further processing. Built staging and mart layers in Redshift for analytics using dbt. Orchestrated the entire workflow with Apache Airflow (MWAA) and managed infrastructure using AWS Secrets Manager, IAM, and boto3 for secure authentication.

<!-- ------------------------- -->


Airfflow Variables examples for your refrence- you need to add these variables in airflow inside admin tab:

S3_raw_bucket = "s3://currency__exhange_raw/"
s3_prefix = "crypto_data"
S3_staging_bucket = "s3://currency__exhange_staging/"
AWS_SECRET_NAME = "dbt_redshift_credentials"
AWS_REGION = "us-east-1"
----------------------------
# API Configuration
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
sample currency api response:

[
  {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 68743,
    "market_cap": 13023456789,
    "total_volume": 567891234,
    "high_24h": 70000,
    "low_24h": 68000,
    "price_change_percentage_24h": -1.23,
    "last_updated": "2024-04-02T12:00:00Z"
  }
]

