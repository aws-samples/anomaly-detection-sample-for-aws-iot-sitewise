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
python3 src/lookout-for-equipment/simulate_real_time_data.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 src/lookout-for-equipment/simulate_real_time_data.py \
  --asset-external-id Workshop_Robot_1-1
'''

import boto3
import json
import os
import time
import random
import argparse

sitewise = boto3.client('iotsitewise')
l4e_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(l4e_dir_path))
project_dir_path = os.path.abspath(os.path.dirname(src_dir_path))

DATA_SIMULATION_DURATION_MINS = 60
DATA_SIMULATION_INTERVAL_SECS = 10

# Load properties configuration
with open(f'{project_dir_path}/config/properties_config.json') as properties_json:
  properties_config = json.load(properties_json)

def batch_put_asset_property_value(entries):
    response = sitewise.batch_put_asset_property_value(entries=entries)

def simulate_data(asset_external_id):
    batch_put_entries = []
    for idx, property in enumerate(properties_config):
        alias = property["alias"].replace("ASSET",asset_external_id.lower())
        line_number = asset_external_id.split("_")[2].split("-")[0]
        alias = alias.replace("LINE", f"line_{line_number}")

        # Simulate data representative of anomalous behavior in joint 1
        if "joint1_current" in alias or "joint1_temperature" in alias:
            min = property["min_anomaly"]
            max = property["max_anomaly"]
        else:
            min = property["min_normal"]
            max = property["max_normal"]
        current_value = round(random.uniform(min, max),2)
        batch_put_entries.append({
            'entryId': str(idx),
            'propertyAlias': alias,
            'propertyValues': [
                {
                    'value': {
                        'doubleValue': current_value
                    },
                    'timestamp': {
                        'timeInSeconds': int(time.time())
                    },
                    'quality': 'GOOD'
                }
            ]
        })
    return batch_put_entries

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()

    asset_external_id = args.asset_external_id
    
    simulation_start_time = int(time.time())
    print(f"Simulating real-time data for the next {DATA_SIMULATION_DURATION_MINS} minutes!\n")
    
    while True:
        current_time = int(time.time())
        # Exit program when simulation duration is reached
        if current_time - simulation_start_time >= DATA_SIMULATION_DURATION_MINS*60:
            print(f"\nSimulation duration reached, exiting program..\n")
            break
        
        batch_put_entries = simulate_data(asset_external_id)
        batch_put_asset_property_value(batch_put_entries)
        print(f"{current_time} - inserted simulated data")
        
        time.sleep(DATA_SIMULATION_INTERVAL_SECS)
        
    print(f"\nScript executed successfully!\n")