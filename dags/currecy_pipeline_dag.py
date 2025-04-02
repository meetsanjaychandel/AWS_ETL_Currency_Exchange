from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AWSGlueJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.dbt.operators.dbt import DbtRunOperationRunOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable


email_list = Variable.get("email_list", deserialize_json=True)
AWS_SECRET_NAME = Variable.get("AWS_SECRET_NAME")
AWS_REGION = Variable.get("AWS_REGION")
DBT_PATH= "usr/local/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/tmp/dbt"

def fetch_secret():
    """Fetch DBT credentials from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=AWS_REGION)
    
    try:
        response = client.get_secret_value(SecretId=AWS_SECRET_NAME)
        secret = json.loads(response["SecretString"])
        
        
    except Exception as e:
        raise Exception(f" Error fetching secret: {str(e)}")

    Variable.set("REDSHIFT_HOST", secret["host"])
    Variable.set("REDSHIFT_USER", secret["username"])
    Variable.set("REDSHIFT_PASSWORD", secret["password"])
    Variable.set("REDSHIFT_DBNAME", secret["redshift_db"])

    print("DBT credentials fetched and stored in Airflow variables.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    'retries': 1,
    'email':email_list,
    'email_on_failure': True,
    'retry_delay': timedelta(minutes=5),
}

@DAG(
    dag_id= 'crypto_data_pipeline',
    default_args=default_args,
    description='End-to-End ETL Pipeline for Crypto Data',
    schedule_interval='@daily',
)

def currency_pipeline():
    extract_task = BashOperator(
        task_id='extract_data_from_api',
        bash_command= f"python /usr/local/airflow/scripts/extract_currency_data.py",

    )
    glue_task = AWSGlueJobOperator(
        task_id='convert_to_parquet',
        job_name='convert_to_parquet',
        aws_conn_id='aws_currency_conn',
        region_name='us-east-1',
        script_location="s3://currency_exchange_glue_scripts/convert_to_parquet.py",
        
    )



    start_data_transformation = BashOperator(
        task_id='start_data_transformation',
        bash_command= f"cp -r /usr/local/airflow/dbt_currency /tmp/dbt; cd /tmp/dbt; ls;",
        
    )

    get_secret_task = PythonOperator(
        task_id='get_dbt_secret',
        python_callable=fetch_secret,
        
    )

    dbt_clean  = BashOperator(
        task_id='dbt_clean',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command= f"{DBT_PATH} clean --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        
    )

    dbt_deps  = BashOperator(
        task_id='dbt_deps',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command= f"{DBT_PATH} deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        
    )

    dbt_debug  = BashOperator(
        task_id='dbt_debug',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command= f"{DBT_PATH} deps --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        
    )

    dbt_ext_tables= BashOperator(
        task_id='dbt_external_tables',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command=(f"{DBT_PATH} run-operation stage_external_sources"
        f"--no-partial --vars  'ext_full_refresh: true' "
        f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}" ),
        
    )

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command= f"{DBT_PATH} run --select staging.* --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        
    )

    dbt_mart = BashOperator(
        task_id='dbt_mart',
        env={
            "REDSHIFT_HOST": "{{var.value.REDSHIFT_HOST}}",
            "REDSHIFT_USER": "{{var.value.REDSHIFT_USER}}",
            "REDSHIFT_PASSWORD": "{{var.value.REDSHIFT_PASSWORD}}",
            "REDSHIFT_DBNAME": "{{var.value.REDSHIFT_DBNAME}}",
        },
        bash_command= f"{DBT_PATH} run --select mart.* --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        
    )

    end_data_transformation = BashOperator(
        task_id='end_data_transformation',
        bash_command= f"cd /usr/local/airflow/dbt_currency; rm -rf /tmp/dbt; ls;",
        
    )

    [start_data_transformation >> get_secret_task >>
    dbt_clean >> dbt_deps >> dbt_debug >> dbt_ext_tables >> 
    dbt_staging >> dbt_mart >> end_data_transformation ]


    extract_task >> glue_task >> start_data_transformation

daily_currency_pipeline = currency_pipeline()