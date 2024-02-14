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
python3 src/lookout-for-equipment/create_l4e_model.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 src/lookout-for-equipment/create_l4e_model.py \
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

PREDICTION_DEFINITION_PREFIX = 'Insuffienct_Lubrication_Contact_issues_'
TRAINING_STATUS_PROPERTY_NAME = "AWS/L4E_ANOMALY_TRAINING_STATUS"
INFERENCE_STATUS_PROPERTY_NAME = "AWS/L4E_ANOMALY_INFERENCE_STATUS"

TRAINING_PROPERTY_EXTERNAL_IDS = [
    "Robot_Joint_1_Current",
    "Robot_Joint_1_Temperature",
    "Robot_Joint_2_Current",
    "Robot_Joint_2_Temperature"
]

# Load schema
with open(f'{l4e_dir_path}/l4e_anomaly_composite_model_properties_schema.json') as schema_json:
  l4e_anomaly_composite_model_properties_schema = json.load(schema_json)
  
# Load configuration
with open(f'{project_dir_path}/config/project_config.yml', 'r') as file:
    config = yaml.safe_load(file)

l4e_config = config["lookout_for_equipment"]

def utc_time_log_prefix():
    current_time_utc = datetime.now(timezone.utc)
    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
    return f'[{formatted_time}] '

def get_property_ids_from_external_ids(asset_model_id, property_external_ids):
    training_property_ids = []

    res = sitewise.describe_asset_model(
    assetModelId=asset_model_id,
    excludeProperties=False)
    properties = res["assetModelProperties"]
    for property in properties:
        if property["externalId"] in property_external_ids:
            training_property_ids.append(property["id"])
    print(f'\n{utc_time_log_prefix()}Retrieved list properties to include in the prediction definition')
    return training_property_ids

def get_model_id_from_asset_external_id(asset_external_id):
    res = sitewise.describe_asset(
        assetId=f"externalId:{asset_external_id}")
    print(f'\n{utc_time_log_prefix()}Retrieved model Id for {asset_external_id}')
    return res["assetModelId"]

def get_asset_id_from_external_id(asset_external_id):
    res = sitewise.describe_asset(
        assetId=f"externalId:{asset_external_id}")
    return res["assetId"]

def get_model_id_from_asset_id(asset_id):
    res = sitewise.describe_asset(assetId=asset_id)
    return res["assetModelId"]

def prepare_composite_model_properties(properties, role_arn, training_property_ids):
    for idx, property in enumerate(properties):
        if property["name"] == "AWS/L4E_ANOMALY_PERMISSIONS":
            properties[idx]["type"]["attribute"] = { "defaultValue": json.dumps({"roleArn": role_arn}) }
        elif property["name"] == "AWS/L4E_ANOMALY_INPUT":
            properties[idx]["type"]["attribute"] = { "defaultValue": json.dumps({"properties": training_property_ids}) }
    return properties
    
def create_prediction_definition(asset_model_id, training_property_ids):

    if len(training_property_ids) == 0: 
        print(f'\n{utc_time_log_prefix()}No relevant properties found in the {asset_model_id} asset model')
        return False
    
    composite_model_properties = prepare_composite_model_properties(l4e_anomaly_composite_model_properties_schema
                                    , l4e_config["role_arn"], training_property_ids)

    response = sitewise.create_asset_model_composite_model(
        assetModelId = asset_model_id,
        assetModelCompositeModelName = f'{PREDICTION_DEFINITION_PREFIX}{int(datetime.now().timestamp())}',
        assetModelCompositeModelType = "AWS/L4E_ANOMALY",
        assetModelCompositeModelProperties = composite_model_properties)

    print(f'\n{utc_time_log_prefix()}Prediction definition submitted, checking status every 5 seconds..')
    while True:  
        res = sitewise.describe_asset_model(assetModelId=asset_model_id,
                                                   excludeProperties=False)
        time.sleep(5)
        if res["assetModelStatus"] == "UPDATING": 
            print(f'\t{utc_time_log_prefix()}Still creating..')
        else:
            print(f'\t{utc_time_log_prefix()}Successfully created!')
            break
    return response["assetModelCompositeModelId"]

def get_action_definition_id_training_inference(asset_model_id, prediction_definition_id):
    actionDefinitionId = None
    response = sitewise.describe_asset_model_composite_model(
        assetModelCompositeModelId = prediction_definition_id,
        assetModelId = asset_model_id
        )
    if "actionDefinitions" in response:
        action_defs = response["actionDefinitions"]
        for action_def in action_defs:
            if action_def["actionName"] == l4e_config["action_name"]:
                actionDefinitionId = action_def["actionDefinitionId"]
    return actionDefinitionId

def get_tracking_property_ids(asset_model_id, prediction_definition_id):
        TRAINING_STATUS_PROPERTY_NAME = "AWS/L4E_ANOMALY_TRAINING_STATUS"
        INFERENCE_STATUS_PROPERTY_NAME = "AWS/L4E_ANOMALY_INFERENCE_STATUS"
        training_status_property_id = None
        inference_status_property_id = None
        
        response = sitewise.describe_asset_model_composite_model(
            assetModelCompositeModelId = prediction_definition_id,
            assetModelId = asset_model_id)
        
        composite_model_properties = response["assetModelCompositeModelProperties"]
        for property in composite_model_properties:
            if property["name"] == TRAINING_STATUS_PROPERTY_NAME:
                training_status_property_id = property["id"]
            elif property["name"] == INFERENCE_STATUS_PROPERTY_NAME:
                inference_status_property_id = property["id"]
        return training_status_property_id, inference_status_property_id

def start_workflow(asset_id, asset_external_id, prediction_definition_id, start_time, end_time):
    print(f"\n{utc_time_log_prefix()}Starting the workflow to create an anomaly detection model")
    asset_model_id = get_model_id_from_asset_id(asset_id)
    actionDefinitionId = get_action_definition_id_training_inference(asset_model_id, prediction_definition_id)

    action_payload_config = {
        "l4ETrainingWithInference": {
            "trainingWithInferenceMode": "START",
            "trainingPayload": {
                "exportDataStartTime": start_time,
                "exportDataEndTime": end_time,
                "labelInputConfiguration": {
                    "bucketName": l4e_config["s3_bucket_name"],
                    "prefix": f"l4e/labels/{asset_external_id.lower()}/"
                    }
            },
            "inferencePayload": {
                "dataDelayOffsetInMinutes": 3,
                "dataUploadFrequency": "PT5M"
            }
        }
    }
    response = sitewise.execute_action(
        actionDefinitionId = actionDefinitionId,
        actionPayload = {
            "stringValue": json.dumps(action_payload_config)
        },
        targetResource = {
            "assetId": asset_id
        }
        )
    print(f"\t{utc_time_log_prefix()}Workflow request submitted")

def poll_workflow_status(asset_id, training_status_property_id, inference_status_property_id):
    print(f"\n{utc_time_log_prefix()}Checking status every 30 seconds..")
    
    while True:
        training_status = None
        inference_status = None
        training_res = sitewise.get_asset_property_value(assetId=asset_id, 
                                                     propertyId=training_status_property_id)
        inferece_res = sitewise.get_asset_property_value(assetId=asset_id, 
                                                     propertyId=inference_status_property_id)
        if "propertyValue" in training_res and "value" in training_res["propertyValue"]:
            training_status = training_res["propertyValue"]["value"]["stringValue"]
            training_status = json.loads(training_status)["status"]
        if "propertyValue" in inferece_res and "value" in inferece_res["propertyValue"]:
            inference_status = inferece_res["propertyValue"]["value"]["stringValue"]
            inference_status = json.loads(inference_status)["status"]
        print(f"\t{utc_time_log_prefix()}Training status: {training_status}, Inference status: {inference_status}")

        if training_status == "L4E_TRAINING_COMPLETED" and inference_status == "L4E_INFERENCE_ACTIVE":
            print(f"{utc_time_log_prefix()}All set! Ready for inference")
            break
        time.sleep(30)

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()

    asset_external_id = args.asset_external_id
    asset_id = get_asset_id_from_external_id(asset_external_id)
    
    asset_model_id = get_model_id_from_asset_external_id(asset_external_id)
    training_property_ids = get_property_ids_from_external_ids(asset_model_id, TRAINING_PROPERTY_EXTERNAL_IDS)
    
    prediction_definition_id = create_prediction_definition(asset_model_id, training_property_ids)
    
    time.sleep(5)

    current_utc_time = datetime.utcnow()
    start_of_today_utc = datetime(current_utc_time.year, current_utc_time.month, current_utc_time.day, tzinfo=timezone.utc)
    
    to_epoch = int(start_of_today_utc.timestamp())
    duration_in_seconds = 30*24*60*60 # 30 days
    from_epoch = to_epoch - duration_in_seconds
    
    start_workflow(asset_id, asset_external_id, prediction_definition_id, from_epoch, to_epoch)
    
    training_status_property_id, inference_status_property_id = get_tracking_property_ids(asset_model_id, prediction_definition_id)
    poll_workflow_status(asset_id, training_status_property_id, inference_status_property_id)
    
    print(f"\n{utc_time_log_prefix()}Script executed successfully!\n")