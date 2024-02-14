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
python3 src/data-bulk-import/simulate_historical_data.py \
  --asset-external-ids <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 src/data-bulk-import/simulate_historical_data.py \
  --asset-external-ids Workshop_Robot_1-1
python3 src/data-bulk-import/simulate_historical_data.py \
  --asset-external-ids Workshop_Robot_1-1 Workshop_Robot_3-1
'''

import random
from datetime import datetime, timezone
import csv
import json
import os
import argparse

import_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(import_dir_path))
project_root_dir_path = os.path.abspath(os.path.dirname(src_dir_path))
data_dir = f'{import_dir_path}/data'
labels_dir = f'{import_dir_path}/labels'

# Load properties configuration
with open(f'{project_root_dir_path}/config/properties_config.json') as properties_json:
  properties_config = json.load(properties_json)
   
# Create directories if doesn't exist
if not os.path.exists(data_dir): os.makedirs(data_dir)
if not os.path.exists(labels_dir): os.makedirs(labels_dir)

def utc_time_log_prefix():
    current_time_utc = datetime.now(timezone.utc)
    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    return f'[{formatted_time}] '

def generate_historical_data(asset_external_id) -> None:
    print(f'\n{utc_time_log_prefix()}Generating simulated historical data for {asset_external_id}..')
    # Get the current time
    current_utc_time = datetime.utcnow()
    start_of_today_utc = datetime(current_utc_time.year, current_utc_time.month, current_utc_time.day, tzinfo=timezone.utc)
    
    to_epoch = int(start_of_today_utc.timestamp())
    duration_in_seconds = 30*24*60*60 # 30 days
    from_epoch = to_epoch - duration_in_seconds

    sampling_seconds = 60*5 # 5 minutes
    data_file_name = f'{asset_external_id.lower()}_historical_data.csv'
    labels_file_name = f'{asset_external_id.lower()}_labels.csv'

    f = open(f'{data_dir}/{data_file_name}', 'w', encoding='UTF8')
    data_writer = csv.writer(f)

    f1 = open(f'{labels_dir}/{labels_file_name}', 'w', encoding='UTF8')
    labels_writer = csv.writer(f1)

    anomaly_duration = sampling_seconds * 24 # 2 hours
    anomaly1_start = to_epoch-25*24*60*60 # 25 days from end
    anomaly2_start = to_epoch-11*24*60*60 # 11 days from end
    
    anomaly1_start_str = datetime.fromtimestamp(anomaly1_start).strftime('%Y-%m-%dT%H:%M:%S.%f')
    anomaly2_start_str = datetime.fromtimestamp(anomaly2_start).strftime('%Y-%m-%dT%H:%M:%S.%f')
    anomaly1_end_str = datetime.fromtimestamp(anomaly1_start+anomaly_duration).strftime('%Y-%m-%dT%H:%M:%S.%f')
    anomaly2_end_str = datetime.fromtimestamp(anomaly2_start+anomaly_duration).strftime('%Y-%m-%dT%H:%M:%S.%f')
 
    print(f"Anomaly 1: {anomaly1_start_str} to {anomaly1_end_str}")
    print(f"Anomaly 2: {anomaly2_start_str} to {anomaly2_end_str}")

    labels_writer.writerow([anomaly1_start_str, anomaly1_end_str])
    labels_writer.writerow([anomaly2_start_str, anomaly2_end_str])
    f1.close()
    print(f'\t{utc_time_log_prefix()}{labels_file_name} file created')
    
    for idx, property in enumerate(properties_config):
        alias = property["alias"].replace("ASSET",asset_external_id.lower())
        line_number = asset_external_id.split("_")[2].split("-")[0]
        alias = alias.replace("LINE", f"line_{line_number}")
        min_normal = property["min_normal"]
        max_normal = property["max_normal"]
        min_anomaly = property["min_anomaly"]
        max_anomaly = property["max_anomaly"]

        for timestamp_seconds in range(from_epoch,to_epoch,sampling_seconds):
            value = None
            # First anomaly 
            if anomaly1_start <= timestamp_seconds <= anomaly1_start+anomaly_duration:
                if 'joint1_current' in property["alias"].lower() \
                    or 'joint1_temperature' in property["alias"].lower():
                    value = round(random.uniform(min_anomaly, max_anomaly),2)
                else:
                    value = round(random.uniform(min_normal, max_normal),2)
            # Second anomaly
            elif anomaly2_start <= timestamp_seconds <= anomaly2_start+anomaly_duration:
                if 'joint2_current' in property["alias"].lower() \
                    or 'joint2_temperature' in property["alias"].lower():
                    value = round(random.uniform(min_anomaly, max_anomaly),2)
                else:
                    value = round(random.uniform(min_normal, max_normal),2)
            # No anomaly
            else:
                value = round(random.uniform(min_normal, max_normal),2)
            
            row = [alias, 'DOUBLE', timestamp_seconds, 0, 'GOOD', value]
            data_writer.writerow(row)

    f.close()
    print(f'\t{utc_time_log_prefix()}{data_file_name} file created')

if __name__ == "__main__":
   # Get argument inputs
   parser = argparse.ArgumentParser()
   parser.add_argument("--asset-external-ids", nargs="+", action="store", required=True)
   args = parser.parse_args()

   asset_external_ids = args.asset_external_ids
   
   for asset_external_id in asset_external_ids:
      generate_historical_data(asset_external_id)
   
   print(f'\n{utc_time_log_prefix()}Script execution successfully completed!\n')