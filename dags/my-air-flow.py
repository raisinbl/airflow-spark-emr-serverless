import os
import boto3
from dotenv import load_dotenv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from utils import create_bucket_acl, create_job_role, delete_job_role, _local_file_to_s3, _local_dir_to_s3

# Load the environment variables
load_dotenv()

BUCKET_NAME=os.getenv("BUCKET_NAME")
print(BUCKET_NAME)
# S3_LOGS_BUCKET = "raisinblack-bk"
REGION=os.getenv('REGION')

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET_NAME}/logs/"}
    },
}
JOB_ROLE_NAME=os.getenv("JOB_ROLE_NAME")
POLICY_NAME=os.getenv("JOB_POLICY_NAME")

APP_CONFIG = {
    "name": "spark-job",
   "architecture": "X86_64",
   "initialCapacity": { 
      "Driver" : { 
         "workerConfiguration": { 
            "cpu": "4vCPU",
            "disk": "20Gb",
            "memory": "12Gb"
         },
         "workerCount": 1
      },
      "Executor" : { 
         "workerConfiguration": { 
            "cpu": "4vCPU",
            "disk": "20Gb",
            "memory": "12Gb"
         },
         "workerCount": 1
      },
   },
   "maximumCapacity": { 
      "cpu": "20vcpu",
      "disk": "200gb",
      "memory": "200gb"
   },
}

pwd = os.getcwd()
local_data = "./dags/data/movie_review.csv"
s3_data = "data/movie_review.csv"
local_script = "dags/scripts/random_text_classification.py"
s3_script = "scripts/random_text_classification.py"
s3_clean = "clean_data/"
s3_output = "output/"


# Create the EMR Serverless application
emr = EmrServerlessHook(aws_conn_id="aws_default")

with DAG(
    dag_id="1-emr-serverless",
    schedule=None,
    start_date=datetime.now(),
    tags=["example"],
    catchup=False,
) as dag:

    # create s3 bucket with acl public-read-write 
    create_bucket = PythonOperator(
        dag=dag,
        task_id="create_bucket",
        python_callable=create_bucket_acl,
        op_kwargs={"bucket_name": BUCKET_NAME},
    )
    
    script_to_s3 = PythonOperator(
        dag=dag,
        task_id="script_to_s3",
        python_callable=_local_dir_to_s3,
        op_kwargs={"local_dir": pwd+"/dags/scripts/", "key": "scripts/", "bucket_name": BUCKET_NAME},
    )

    data_to_s3 = PythonOperator(
    dag=dag,
    task_id="data_to_s3",
    python_callable=_local_file_to_s3,
    op_kwargs={"filename": local_data, "key": s3_data, "bucket_name": BUCKET_NAME},
    )

    create_job_role_operator = PythonOperator(
        task_id="create_job_role",
        python_callable=create_job_role,
        op_kwargs={"role_name": JOB_ROLE_NAME, "policy_name": POLICY_NAME},
    )

    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_app",
        job_type="SPARK",
        release_label="emr-6.10.0",
        config=APP_CONFIG,
    )


    classify = EmrServerlessStartJobOperator(
        task_id="classify",
        name="spark-job-classify",
        application_id=create_app.output, # this equal to application="{{ task_instance.xcom_pull('create_app') }}", 
        execution_role_arn=create_job_role_operator.output,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{BUCKET_NAME}/{s3_script}",
                "entryPointArguments": [f"--input",f"s3://{BUCKET_NAME}/{s3_data}", 
                                        "--output", f"s3://{BUCKET_NAME}/{s3_output}"],
                "sparkSubmitParameters": f"--conf spark.archives=s3://{BUCKET_NAME}/{os.path.dirname(s3_script)}/pyspark_venv.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=create_app.output, # this equal to application="{{ task_instance.xcom_pull('create_app') }}",
        trigger_rule="all_success",
    )

    delete_job_role_operator = PythonOperator(
        task_id="delete_job_role",
        python_callable=delete_job_role,
        op_kwargs={"role_name": JOB_ROLE_NAME, "policy_name": POLICY_NAME},
        trigger_rule="all_success",
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        force_delete=True,
        aws_conn_id="aws_default",
        trigger_rule="all_success",
    )

    create_bucket >> [data_to_s3, script_to_s3 , create_job_role_operator, create_app] >> classify >> (delete_app , delete_job_role_operator, delete_bucket) 