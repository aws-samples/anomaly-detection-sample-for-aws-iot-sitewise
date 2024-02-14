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
python3 ~/workshop/cleanup/remove_l4e_resources.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 ~/workshop/cleanup/remove_l4e_resources.py \
  --asset-external-id Workshop_Robot_1-1
'''

import os
import boto3
import time
import argparse

sitewise = boto3.client('iotsitewise')
l4e = boto3.client('lookoutequipment')

inference_scheduler_names = []
model_names = []
dataset_names = []

cleanup_root_dir = os.path.abspath(os.path.dirname(__file__))
workshop_dir_path = os.path.abspath(os.path.dirname(cleanup_root_dir))

def internal_from_external_asset_id(external_asset_id):
    asset_id = None
    try:
        res = sitewise.describe_asset(assetId=f'externalId:{external_asset_id}')
        asset_id = res["assetId"]
    except sitewise.exceptions.ResourceNotFoundException: 
        print(f'\n{external_asset_id} asset does not exist, no models and assets to remove')
    return asset_id

def dataset_names_from_asset(asset_id):
    max_results = 500
    next_token = None

    while True:
        if next_token:
            response = l4e.list_datasets(MaxResults=max_results, NextToken=next_token)
        else:
            response = l4e.list_datasets(MaxResults=max_results)
        dataset_summaries = response["DatasetSummaries"]
        for dataset_summary in dataset_summaries:
            if asset_id in dataset_summary["DatasetName"]:
                dataset_names.append(dataset_summary["DatasetName"])
        if "NextToken" in response:
            next_token = response["NextToken"]
        else:
            break

def model_names_from_dataset(dataset_name):
    response = l4e.list_models(MaxResults=500, DatasetNameBeginsWith=dataset_name)
    model_summaries = response["ModelSummaries"]
    for model_summary in model_summaries:
        model_names.append(model_summary["ModelName"])

def inference_scheduler_names_from_model(model_name):
    response = l4e.list_inference_schedulers(MaxResults=500, ModelName=model_name)
    inference_scheduler_summaries = response["InferenceSchedulerSummaries"]
    for inference_scheduler_summary in inference_scheduler_summaries:
        inference_scheduler_names.append(inference_scheduler_summary["InferenceSchedulerName"])

def inference_scheduler_status(inference_scheduler_name):
    response = l4e.describe_inference_scheduler(InferenceSchedulerName=inference_scheduler_name)
    return response["Status"]

def stop_inference_scheduler(inference_scheduler_name):
    l4e.stop_inference_scheduler(InferenceSchedulerName=inference_scheduler_name)

def stop_inference_schedulers():
    count = 0
    for inference_scheduler_name in inference_scheduler_names:
        if inference_scheduler_status(inference_scheduler_name) == "RUNNING":
            count += 1
            stop_inference_scheduler(inference_scheduler_name)
            while True:
                time.sleep(2)
                if inference_scheduler_status(inference_scheduler_name) == "STOPPED": 
                    print(f'\tStopped inference scheduler: {inference_scheduler_name}')
                    break
    if count == 0: print(f'\tNothing to stop!')
                    
def delete_inference_schedulers():
    count = 0
    for inference_scheduler_name in inference_scheduler_names:
        count += 1
        l4e.delete_inference_scheduler(InferenceSchedulerName=inference_scheduler_name)
        print(f'\tDeleted inference scheduler: {inference_scheduler_name}')
    if count == 0: print(f'\tNothing to delete!')

def delete_models():
    count = 0
    for model_name in model_names:
        count += 1
        l4e.delete_model(ModelName=model_name)
        print(f'\tDeleted model: {model_name}')
    if count == 0: print(f'\tNothing to delete!')

def delete_datasets():
    count = 0
    for dataset_name in dataset_names:
        count += 1
        l4e.delete_dataset(DatasetName=dataset_name)
        print(f'\tDeleted dataset: {dataset_name}')
    if count == 0: print(f'\tNothing to delete!')

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()

    ## Models and Assets
    asset_external_id = args.asset_external_id
    asset_id = internal_from_external_asset_id(asset_external_id)

    if asset_id:
        dataset_names_from_asset(asset_id)
        for dataset_name in dataset_names:
            model_names_from_dataset(dataset_name)
        for model_name in model_names:
            inference_scheduler_names_from_model(model_name)
        
        print(f'\nStopping relevant inference schedulers..')
        stop_inference_schedulers()
        print(f'\nRemoving relevant inference schedulers..')
        delete_inference_schedulers()
        print(f'\nRemoving relevant models..')
        delete_models()
        print(f'\nRemoving relevant datasets..')
        delete_datasets()
    else:
        print(f'\nAsset does not exist, no l4e resources to remove')

    print(f'\nScript executed successfully!\n')