from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



with DAG("AY_data_pipleine",start_date=datetime(2023,5,27),
    schedule_interval="@daily",default_args=default_args, catchup=False) as dag:
    run_spark_job = SparkSubmitOperator(
        task_id = "run_spark_job",
        application = "/opt/airflow/dags/scripts/forex_processing.py",
        conn_id = "spark_conn",
        verbose = False
    )