# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Usage:
'''
python3 src/data-bulk-import/import_historical_data.py
'''

import os
from datetime import datetime, timezone
import time
from typing import List, Dict
import glob
import yaml
import boto3
import shutil

sitewise = boto3.client('iotsitewise')
s3 = boto3.client('s3')

import_root_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(import_root_dir_path))
project_root_dir_path = os.path.abspath(os.path.dirname(src_dir_path))
data_dir = f'{import_root_dir_path}/data'
labels_dir = f'{import_root_dir_path}/labels'
DATA_COLUMN_NAMES = ["ALIAS", "DATA_TYPE", "TIMESTAMP_SECONDS", "TIMESTAMP_NANO_OFFSET", "QUALITY", "VALUE"]

job_ids = []

# Load configuration
with open(f'{project_root_dir_path}/config/project_config.yml', 'r') as file:
    project_config = yaml.safe_load(file)
    data_import_config = project_config["data_import"]
    lookout_for_equipment_config = project_config["lookout_for_equipment"]

def utc_time_log_prefix() -> str:
    current_time_utc = datetime.now(timezone.utc)
    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    return f'[{formatted_time}] '

def delete_s3_objects_with_prefix(bucket_name, prefix) -> None:
    # List objects in the specified S3 bucket with the given prefix
    objects_to_delete = []
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Check if any objects were found
    if 'Contents' in response:
        objects_to_delete = response['Contents']

    # Delete each object with the specified prefix
    for s3_object in objects_to_delete:
        s3.delete_object(Bucket=bucket_name, Key=s3_object['Key'])

def upload_history_to_s3(s3_bucket_name) -> None:
    # Delete any existing objects
    print(f'\n{utc_time_log_prefix()}Deleting any existing data files in Amazon S3..')
    delete_s3_objects_with_prefix(s3_bucket_name, data_import_config["data_prefix"])

    print(f'\n{utc_time_log_prefix()}Uploading historical data files into Amazon S3..')
    data_files = glob.glob(os.path.join(data_dir, "*"))
    #batch_timestamp = str(int(datetime.now().timestamp()))
    for idx, local_file_path in enumerate(data_files):
        file_name = local_file_path.split('/')[-1]
        s3_key = f'{data_import_config["data_prefix"]}{file_name}'
        s3.upload_file(local_file_path, s3_bucket_name, s3_key)
    print(f'\t{utc_time_log_prefix()}Successfully uploaded to S3!')

def upload_labels_to_s3(s3_bucket_name) -> None:
    # Delete any existing objects
    print(f'\n{utc_time_log_prefix()}Deleting any existing labels in Amazon S3..')
    delete_s3_objects_with_prefix(s3_bucket_name, lookout_for_equipment_config["labels_prefix"])

    print(f'\n{utc_time_log_prefix()}Uploading labels to Amazon S3..')
    label_files = glob.glob(os.path.join(labels_dir, "*"))
    #batch_timestamp = str(int(datetime.now().timestamp()))
    for idx, local_file_path in enumerate(label_files):
        file_name = local_file_path.split('/')[-1]
        asset_external_id = file_name.replace("_labels.csv","")
        s3_key = f'{lookout_for_equipment_config["labels_prefix"]}{asset_external_id}/{file_name}'
        s3.upload_file(local_file_path, s3_bucket_name, s3_key)
    print(f'\t{utc_time_log_prefix()}Successfully uploaded to S3!')
    
def get_s3_keys(s3_bucket) -> List[str]:
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=data_import_config["data_prefix"])
    s3_keys = []
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        content_records = response["Contents"]
        s3_keys = [record["Key"] for record in content_records]
    return s3_keys

def create_job(s3_bucket, s3_key, role_arn) -> str:
    response = sitewise.create_bulk_import_job(
        jobName= f'job_{str(int(datetime.now().timestamp()))}',
        jobRoleArn=role_arn,
        files=[
            {
                'bucket': s3_bucket,
                'key': s3_key
            },
        ],
        errorReportLocation={
            'bucket': s3_bucket,
            'prefix': data_import_config["error_prefix"]
        },
        jobConfiguration={
            'fileFormat': {
                'csv': {
                    'columnNames': DATA_COLUMN_NAMES
                }
            }
        }
    )
    return response['jobId']

def create_jobs(s3_bucket, role_arn) -> None:
    s3_keys = get_s3_keys(s3_bucket)
    print(f'\n{utc_time_log_prefix()}Total objects in S3: {len(s3_keys)}')
    if len(s3_keys) > 0: 
        print(f'{utc_time_log_prefix()}Number of bulk import jobs to create: {len(s3_keys)}')
    else:
        print(f'\n{utc_time_log_prefix()}No data found in S3!')

    for s3_key in s3_keys:
        job_id = create_job(s3_bucket, s3_key, role_arn)
        print(f'\t{utc_time_log_prefix()}Created job: {job_id} to import data from {s3_key} S3 object')
        job_ids.append(job_id)
        time.sleep(1)
    print(f'\n{utc_time_log_prefix()}Completed creating jobs')

def list_bulk_import_jobs() -> List[Dict]:
    all_jobs = []
    response = sitewise.list_bulk_import_jobs(maxResults=250)
    all_jobs = response["jobSummaries"]
    while True:
        # If there are more jobs, get the next page of results
        if 'nextToken' in response:
            response = sitewise.list_bulk_import_jobs(nextToken=response['nextToken'])
            all_jobs = all_jobs + response["jobSummaries"]
        else:
            break  # No more jobs, exit the loop
    return all_jobs

def job_status(job_id: str) -> str:
    status = None
    for job in list_bulk_import_jobs():
        if job['id'] == job_id: status = job["status"] 
    return status

def check_job_status() -> None:
    SLEEP_SECS = 10
    active_job_ids = job_ids.copy()
    print(f'\n{utc_time_log_prefix()}Checking job submission status every {SLEEP_SECS} secs..')

    while True:
        for job_id in active_job_ids:
            status=job_status(job_id)
            if status not in ['PENDING','RUNNING']:
                print(f'\t{utc_time_log_prefix()}Job id: {job_id}, Submission status: {status}')
                active_job_ids.remove(job_id)
        if len(active_job_ids) == 0: 
            break
        time.sleep(SLEEP_SECS)

    print(f'\n{utc_time_log_prefix()}Completed submitting job(s)')

def clean_up_data_dir()  -> None:
    if os.path.exists(data_dir): 
        shutil.rmtree(data_dir)    
        print(f'\n{utc_time_log_prefix()}Removed all the files under {data_dir}')
    if os.path.exists(labels_dir): 
        shutil.rmtree(labels_dir)    
        print(f'\n{utc_time_log_prefix()}Removed all the files under {labels_dir}')

if __name__ == "__main__":
    print(f"Loading from config file..")
    data_import_s3_bucket_name = data_import_config["s3_bucket_name"]
    l4e_s3_bucket_name = lookout_for_equipment_config["s3_bucket_name"]
    role_arn = data_import_config["role_arn"]
            
    upload_history_to_s3(data_import_s3_bucket_name)
    upload_labels_to_s3(l4e_s3_bucket_name)
    create_jobs(data_import_s3_bucket_name, role_arn)
    check_job_status()
    clean_up_data_dir()

    print(f'\n{utc_time_log_prefix()}Script execution successfully completed!\n')
