# Service
import os
import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv
load_dotenv()

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
    try:
        s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)
    except PermissionError:
        # change the permission of the file
        os.chmod(filename, 0o755)
        s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

def _local_dir_to_s3(local_dir, key, bucket_name):
    s3 = S3Hook()
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(key, relative_path)
            s3.load_file(local_path, s3_key, bucket_name=bucket_name, replace=True)
