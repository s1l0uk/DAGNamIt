from datetime import timedelta
import urllib

import airflow
from airflow.operators import PythonOperator
from airflow.models import DAG
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import TransferConfig
import boto3
import paramiko


BUCKET = 'mobg-test-data-raw'

def upload(key):
    # set up sftp session
    transport = paramiko.Transport(("10.0.3.124", 2222))
    transport.connect(username="foo", password="pass")
    sftp = paramiko.SFTPClient.from_transport(transport)

    # setup aws boto3 client
    s3_client = boto3.client("s3")
    transfer = S3Transfer(s3_client)

    # generate tags based on file name
    key_split = key.replace('.nc', '').split('_')
    tags = {
        'TagSet': [
            { 'Key': 'source', 'Value': 'UK Met Office' },
            { 'Key': 'category', 'Value': 'Weather Forecast Model' },
            { 'Key': 'format', 'Value': 'netCDF' },
            { 'Key': 'model', 'Value': key_split[2] },
            { 'Key': 'reference_date', 'Value': key_split[3] },
            { 'Key': 'reference_time', 'Value': key_split[4] },
            { 'Key': 'realisation', 'Value': key_split[5] },
            { 'Key': 'forecast_period', 'Value': key_split[6] }
        ]
    }

    # open ftp file as file like object
    config = TransferConfig(multipart_threshold=4096, max_concurrency=10)
    s3_client.upload_fileobj(sftp.open("upload/" + key), BUCKET, key, Config=config)
    s3_client.put_object_tagging(Bucket=BUCKET, Key=key, Tagging=tags)


def ftp():

    # set up sftp session
    transport = paramiko.Transport(("10.0.3.124", 2222))
    transport.connect(username="foo", password="pass")
    sftp = paramiko.SFTPClient.from_transport(transport)

    # setup aws boto3 client
    s3_client = boto3.client("s3")

    # list files in s3 bucket
    s3_files = set(
        key.get('Key', None) for key in
        s3_client.list_objects_v2(Bucket=BUCKET).get('Contents', [])
    )
    print(s3_files)

    # list files in ftp server
    ftp_files = set(
        key for key in sftp.listdir("upload")
        if key.lower().endswith(('.pp', '.nc'))
    )
    print(ftp_files)

    # find the files in ftp but not in s3
    upload_files = ftp_files - s3_files
    print(upload_files)

    # upload each file in list of files to uploaded
    for key in upload_files:
        print('Uploading '+key)
        upload(key)


dag = DAG(
    dag_id='ftp-to-s3',
    default_args={
        'owner': 'airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    },
    schedule_interval='* * * * *',
    dagrun_timeout=timedelta(minutes=60),
    catchup = False,
    max_active_runs=1
)

task = PythonOperator(
    task_id='ftp',
    python_callable=ftp,
    dag=dag
)

if __name__ == "__main__":
    dag.cli()

