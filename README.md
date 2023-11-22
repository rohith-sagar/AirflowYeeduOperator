# Airflow Yeedu Operator

## Installation

To use the Yeedu Operator in your Airflow environment, install it using the following command:

```bash
sudo pip install yeedu-operator-test
```

# Example DAG

```
from airflow import DAG
from datetime import datetime, timedelta
from yeedu.operators.yeedu import YeeduJobRunOperator

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'yeedu_spark_job_final',
    default_args=default_args,
    description='DAG to submit Spark job to Yeedu',
    schedule_interval='@once',  # You can modify the schedule_interval as needed
)

# YeeduOperator task to submit the Spark job
yeedu_task = YeeduJobRunOperator(
    task_id='submit_yeedu_spark_job_server_final',
    job_conf_id=70,
    token='',
    hostname='',
    workspace_id=3,
    dag=dag,
)

```


