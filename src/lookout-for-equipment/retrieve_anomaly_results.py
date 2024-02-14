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
python3 src/lookout-for-equipment/retrieve_anomaly_results.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 src/lookout-for-equipment/retrieve_anomaly_results.py \
  --asset-external-id Workshop_Robot_1-1
'''

import boto3
from datetime import datetime, timezone
import json
import yaml
import os
import time
import argparse

sitewise = boto3.client('iotsitewise')
l4e_dir_path = os.path.abspath(os.path.dirname(__file__))
src_dir_path = os.path.abspath(os.path.dirname(l4e_dir_path))
project_dir_path = os.path.abspath(os.path.dirname(src_dir_path))

def describe_asset_model(asset_model_id):
    res = sitewise.describe_asset_model(
        assetModelId=asset_model_id,
        excludeProperties=False
        )
    return res
    
def describe_asset(asset_external_id):
    res = sitewise.describe_asset(
        assetId=f'externalId:{asset_external_id}',
        excludeProperties=True
        )
    return res

def describe_asset_model_composite_model (asset_model_id, asset_model_composite_model_id):
    res = sitewise.describe_asset_model_composite_model(
        assetModelId=asset_model_id,
        assetModelCompositeModelId=asset_model_composite_model_id
        )
    return res

def get_property_name_from_id(asset_id, property_id):
    res = sitewise.describe_asset_property(
        assetId=asset_id,
        propertyId=property_id
        )
    return res["assetProperty"]["name"]
              
def get_raw_anomaly_result(assetId, propertyId):
    result = None
    response = sitewise.get_asset_property_value(
        assetId=assetId,
        propertyId=propertyId
    )
    if "propertyValue" in response and "value" in response["propertyValue"] and "stringValue" in response["propertyValue"]["value"]:
        result = response["propertyValue"]["value"]["stringValue"]
    return result

def get_prediction_definitions(asset_model_id):
    pred_defs = []
    asset_model_res = describe_asset_model(asset_model_id)

    if 'assetModelCompositeModelSummaries' in asset_model_res:
        composite_model_summaries = asset_model_res["assetModelCompositeModelSummaries"]
        for composite_model_summary in composite_model_summaries:
            if composite_model_summary["type"] == "AWS/L4E_ANOMALY":
                pred_defs.append(composite_model_summary)
    return pred_defs

def get_anomaly_result_property_id(asset_model_id, asset_model_composite_model_id):
    anomaly_result_property_id = None
    res = describe_asset_model_composite_model(asset_model_id, asset_model_composite_model_id)
    composite_model_properties = res["assetModelCompositeModelProperties"]
    for property in composite_model_properties:
        if property["name"] == "AWS/L4E_ANOMALY_RESULT":
            anomaly_result_property_id = property["id"]
    return anomaly_result_property_id

def get_converted_diagnostics(asset_id, diagnostics):
    converted_diagnostics = []
    for item in diagnostics:
        property_id = item["name"].split('\\')[0]
        value = item["value"]
        property_name = get_property_name_from_id(asset_id, property_id)
        converted_diagnostics.append({
         "name": property_name, 
         "value": value
        })
    return converted_diagnostics
    
def print_anomaly_results(asset_external_id):
    res = describe_asset(asset_external_id)
    asset_model_id = res["assetModelId"]
    asset_id = res["assetId"]
    
    pred_defs = get_prediction_definitions(asset_model_id)
    pred_defs_count = len(pred_defs)
    if pred_defs_count == 0:
        print("No prediction definition found!\n")
        exit(0)
    else:
        print(f"Found {pred_defs_count} prediction definition(s)\n")
    
    print(f"Retrieving anomaly results for each prediction definition..")
    
    for pred_def in pred_defs:
        pred_def_name = pred_def["name"]
        pred_def_id = pred_def["id"]
        print(f"\tPrediction definition: {pred_def_name}")
        anomaly_result_property_id = get_anomaly_result_property_id(asset_model_id, pred_def_id)
        anomaly_result_string = get_raw_anomaly_result(asset_id, anomaly_result_property_id)
        if anomaly_result_string:
            anomaly_result = json.loads(anomaly_result_string)
            converted_diagnostics = get_converted_diagnostics(asset_id, anomaly_result["diagnostics"])
            print(f'\t\tPrediction: {anomaly_result["prediction_reason"]}')
            print(f'\t\tAnomaly Score: {anomaly_result["anomaly_score"]}')
            print(f'\t\tContributing Sensors')
            for sensor in converted_diagnostics:
                print(f'\t\t\t{sensor["name"]}: {round(sensor["value"]*100,1)} %')
        else:
            print(f'\t\tNo anomaly result available')

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()

    asset_external_id = args.asset_external_id
    
    print_anomaly_results(asset_external_id)
    print(f"\nScript executed successfully!\n")