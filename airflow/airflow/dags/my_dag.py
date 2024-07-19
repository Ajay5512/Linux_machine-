from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 3,
}

with DAG('spark_operator', default_args=default_args, schedule_interval=None) as dag:

    python_job = SparkSubmitOperator(
        task_id='python_job',
        application='/plugins/pyspark.py',
        conn_id='spark_default',  # Make sure this connection exists in Airflow
        conf={
            'spark.master': 'local[*]',  # Using local mode
        },
        name='arrow-spark',
        verbose=True,
        env_vars={
            'SPARK_MASTER_HOST': '172.19.0.2',  # This may not be needed for local mode
        },
    )

    python_job
