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
python3 src/data-bulk-import/verify_historical_data.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 src/data-bulk-import/verify_historical_data.py \
  --asset-external-id Workshop_Robot_1-1
'''

import os
from datetime import datetime, timezone
import time
from typing import List, Dict
import glob
import yaml
import boto3
import shutil
import argparse

sitewise = boto3.client('iotsitewise')
s3 = boto3.client('s3')

import_root_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(import_root_dir_path))
project_root_dir_path = os.path.abspath(os.path.dirname(src_dir_path))

# Load configuration
with open(f'{project_root_dir_path}/config/project_config.yml', 'r') as file:
    config = yaml.safe_load(file)

def get_total_tvq_count(alias):
    tvqs = []
    end_time = int(time.time())
    start_time = end_time - 30*24*60*60  #30 days, 
    response = sitewise.get_asset_property_value_history(
    propertyAlias=alias,
    startDate = start_time,
    endDate = end_time,
    maxResults=20000
    )
    tvqs = response["assetPropertyValueHistory"]

    while True:
        if "nextToken" in response:
            response = sitewise.get_asset_property_value_history(
                propertyAlias=alias,
                startDate = start_time,
                endDate = end_time,
                maxResults=20000,
                nextToken=response["nextToken"]
                )
            tvqs = tvqs + response["assetPropertyValueHistory"]
            time.sleep(1)
        else: break
    return len(tvqs)

def verify_historical_data(asset_external_id):
    response = sitewise.describe_asset(assetId=f'externalId:{asset_external_id}'
                                       , excludeProperties=False)
    properties = response["assetProperties"]

    # Checking only measurements of this code sample. You can also use transforms and metrics.
    print(f"Checking for 30 days of historical data for measurements..")
    alias_count = 0
    for property in properties:
        if "alias" in property:
            alias_count += 1
            name = property["name"]
            alias = property["alias"]
            tvq_count = get_total_tvq_count(alias)
            print(f"\tMeasurement name: {name}, Data points: {tvq_count}")
            
    if alias_count == 0: print(f"No measurements found!")

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()
    asset_external_id = args.asset_external_id
                    
    verify_historical_data(asset_external_id) 
    print(f'\nScript execution successfully completed!\n')