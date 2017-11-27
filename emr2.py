from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


JOB_FLOW_ID = 'j-2ILKJ4LKWUW6G'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_STEPS = [
    {
        'Name': 'trajectory-planning',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.pyspark.python=/home/hadoop/conda/bin/python',
                '--py-files',
                's3://mobg-test-emr/tp.zip',
                's3://mobg-test-emr/run.py'
            ],
        },
        'ActionOnFailure': 'CONTINUE'
    }]

dag = DAG(
    'emr',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id=JOB_FLOW_ID,
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id=JOB_FLOW_ID,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

step_adder.set_downstream(step_checker)
