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
python3 src/metadata-bulk-import/import_sitewise_models_assets.py \
  --s3-bucket <S3_BUCKET_NAME> \
  --definitions-file-name <DEFINITION_FILE_NAME>
'''
# Examples:
'''
python3 src/metadata-bulk-import/import_sitewise_models_assets.py \
  --s3-bucket my-bucket-name-123 \
  --definitions-file-name definitions_models_assets.json
'''

import os
from datetime import datetime, timezone
import time
import argparse
import yaml
import boto3

import_root_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(import_root_dir_path))
project_root_dir_path = os.path.abspath(os.path.dirname(src_dir_path))

twinmaker = boto3.client('iottwinmaker')
s3 = boto3.client('s3')
sitewise = boto3.client('iotsitewise')

# Load configuration
with open(f'{project_root_dir_path}/config/project_config.yml', 'r') as file:
    config = yaml.safe_load(file)

job_id = f'Sample_Bulkimport_{int(datetime.now().timestamp())}'

def utc_time_log_prefix():
    current_time_utc = datetime.now(timezone.utc)
    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    return f'[{formatted_time}] '

def confirm_assets_do_not_exist(asset_model_id):
    assets_do_not_exist = False
    while True:  
        res = sitewise.list_assets(assetModelId=asset_model_id)
        if len(res["assetSummaries"]) == 0:
            assets_do_not_exist = True
            break
    return assets_do_not_exist

# Upload a local file to S3
def upload_file_to_s3(local_file_path: str, bucket: str, s3_key: str) -> None:
    """Upload a local file to S3 bucket
    """
    s3.upload_file(local_file_path, bucket, s3_key)
    print(f'\n{utc_time_log_prefix()}Uploaded metadata bulk import schema JSON file to {bucket} S3 bucket!')

# Create a bulk import/export job
def create_metadata_job(job_id: str, source_type: str, destination_type: str, s3_bucket: str, s3_key: str) -> None:
    twinmaker.create_metadata_transfer_job(
        metadataTransferJobId = job_id,
        sources = [{
            'type': source_type,
            's3Configuration': {
                'location': f'arn:aws:s3:::{s3_bucket}/{s3_key}'
            }
        }],
        destination = {
            'type': destination_type
        }    
    )
    print(f'\n{utc_time_log_prefix()}Created metadata bulk job with job Id: {job_id}')
    return True

# Print status of a given job id
def print_job_status(job_id):
    print(f'\n{utc_time_log_prefix()}Checking status of {job_id} job every 30 seconds..')
    print(f'{utc_time_log_prefix()}Tip: You can also check the status from AWS IoT SiteWise console')
    while True:
        res = twinmaker.get_metadata_transfer_job(metadataTransferJobId=job_id)
        state = res["status"]["state"]
        if state in ('RUNNING', 'COMPLETED'):
            try:
                progress = res["progress"]
                print(f'\t{utc_time_log_prefix()}Status: {state} | Total: {progress["totalCount"]}, Suceeded: {progress["succeededCount"]}, Skipped: {progress["skippedCount"]}, Failed: {progress["failedCount"]}')                
            except:
                print(f'\t{utc_time_log_prefix()}Status: {state}')
        elif state == 'ERROR':
            try:
                reportUrl = res["reportUrl"]
                print(f'\t{utc_time_log_prefix()}Status: {state} | Report URL: {reportUrl}')
            except:
                print(f'\t{utc_time_log_prefix()}Status: {state}')
            break
        else:
            print(f'\t{utc_time_log_prefix()}Status: {state}')
        time.sleep(30) # Check status every 30 seconds
        if state == 'COMPLETED': 
            print(f'\n{utc_time_log_prefix()}{job_id} job successfully completed!')
            print(f'{utc_time_log_prefix()}Tip: You can verify the changes in AWS IoT SiteWise console')
            break

def disassociate_mapped_data_streams(asset_id):
    if not asset_id:
        #print('\nNothing to disassociate as no asset found!..')
        return True
    response = sitewise.describe_asset(
    assetId=asset_id,excludeProperties=False)
    #print('\nDisassociating data streams from properties..')
    properties = response["assetProperties"]
    for property in properties:
        if "alias" in property and property["alias"]:
            diassociate_data_stream_from_property(property["alias"], 
                asset_id, property["id"])
            #print(f'\tDisassociated {property["alias"]} data stream from {asset_id} asset')
    time.sleep(5) # Sleep before checking asset status
    while True:  
        res = sitewise.describe_asset(assetId=asset_id,excludeProperties=False)
        if res["assetStatus"] != "UPDATING": 
            #print('Disassociated data streams from properties')
            break
    return True

def diassociate_data_stream_from_property(alias, asset_id, property_id):
    sitewise.disassociate_time_series_from_asset_property(
            alias=alias,
            assetId=asset_id,
            propertyId=property_id
    )

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--definitions-file-name", action="store", required=True, dest="definitions_file_name")
    parser.add_argument("--s3-bucket", action="store", dest="s3_bucket_name")
    args = parser.parse_args()

    definitions_file_name = args.definitions_file_name
    s3_bucket_name = args.s3_bucket_name
    if not s3_bucket_name:
        print("No --s3-bucket argument provided, looking from config file..")
        if config["metadata_bulk_operations"]["s3_bucket_name"]:
            print(f"\tFound S3 bucket name from config file")
            s3_bucket_name = config["metadata_bulk_operations"]["s3_bucket_name"]

    metadata_config_file_path = f'{import_root_dir_path}/{definitions_file_name}'
    metadata_config_file_S3_key = f'metadata-bulk-import/{definitions_file_name}'
    
    upload_file_to_s3(metadata_config_file_path, s3_bucket_name, metadata_config_file_S3_key)
    create_metadata_job(job_id, 's3', 'iotsitewise', s3_bucket_name, metadata_config_file_S3_key)
    print_job_status(job_id)
    print(f'\n{utc_time_log_prefix()}Congratulations! script execution completed.\n')