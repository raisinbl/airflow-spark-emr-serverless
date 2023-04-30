import os
import boto3
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

BUCKET_NAME="raisin-bk"
S3_LOGS_BUCKET = "raisinblack-bk"
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

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{BUCKET_NAME}/logs/"}
    },
}
JOB_ROLE_NAME="EMRServerless_AccessS3Role"
POLICY_NAME="EMRServerless_AccessS3Policy"

pwd = os.getcwd()
local_data = "./dags/data/movie_review.csv"
s3_data = "data/movie_review.csv"
local_script = "dags/scripts/random_text_classification.py"
s3_script = "scripts/random_text_classification.py"
s3_clean = "clean_data/"
s3_output = "output/"

# helper function
iam = boto3.client('iam')
sts = boto3.client('sts')
s3 = boto3.client('s3')

ACCOUNT_ID = sts.get_caller_identity()['Account']

# Create the role and policy
def create_job_role(role_name, policy_name):
    ROLE_ARN=f'arn:aws:iam::{ACCOUNT_ID}:role/{role_name}'
    POLICY_ARN=f'arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}'
    #Create the IAM role
    try:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument='''{
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "emr-serverless.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }'''
        )
    except iam.exceptions.EntityAlreadyExistsException:
        role = iam.get_role(RoleName=role_name)

    # Create the custom policy
    try:
        policy = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument='''{
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "s3:*",
                        "Resource": "*",
                        "Sid": "s3Access"
                    }
                ]
            }''',
            Description='Allows EMR Serverless to access to all resources'
        )
    except iam.exceptions.EntityAlreadyExistsException:
        policy = iam.get_policy(PolicyArn=POLICY_ARN)

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=POLICY_ARN
    )
    return ROLE_ARN

# Delete the role and policy
def delete_job_role(role_name, policy_name):
    POLICY_ARN=f'arn:aws:iam::{ACCOUNT_ID}:policy/{policy_name}'
    # detach the policy
    iam.detach_role_policy(
        RoleName=role_name,
        PolicyArn=POLICY_ARN
    )

    # delete the policy
    iam.delete_policy(
        PolicyArn=POLICY_ARN
    )

    # delete the role
    iam.delete_role(
        RoleName=role_name
    )

def create_bucket_acl(bucket_name):

    s3.create_bucket(
        Bucket=bucket_name,
        ObjectOwnership='ObjectWriter'
    )
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': False,
            'IgnorePublicAcls': False,
            'BlockPublicPolicy': False,
            'RestrictPublicBuckets': False
        }
    )
    s3.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

    return bucket_name

# helper function
def _local_file_to_s3(filename, key, bucket_name):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

def _local_dir_to_s3(local_dir, key, bucket_name):
    s3 = S3Hook()
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(key, relative_path)
            s3.load_file(local_path, s3_key, bucket_name=bucket_name, replace=True)


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
        bucket_name=S3_LOGS_BUCKET,
        aws_conn_id="aws_default",
        trigger_rule="all_success",
    )

    create_bucket >> [data_to_s3, script_to_s3 , create_job_role_operator, create_app] >> classify >> (delete_app , delete_job_role_operator, delete_bucket) 
