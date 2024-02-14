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
python3 ~/workshop/cleanup/remove_sitewise_resources.py \
  --asset-external-id <ASSET_EXTERNAL_ID>
'''
# Examples:
'''
python3 ~/workshop/cleanup/remove_sitewise_resources.py \
  --asset-external-id Workshop_Corporate_AnyCompany
'''

import boto3
import time
import argparse

client = boto3.client('iotsitewise')

asset_model_ids_to_delete = []
asset_ids_to_delete = []

def internal_from_external_asset_id(external_asset_id):
    asset_id = None
    try:
        res = client.describe_asset(assetId=f'externalId:{external_asset_id}')
        asset_id = res["assetId"]
    except client.exceptions.ResourceNotFoundException: 
        print(f'\n{external_asset_id} asset does not exist!')
    return asset_id

def confirm_model_update_complete(asset_model_id):
    complete_status = False
    time.sleep(2) # wait a few seconds for the status to update
    while True:
        response = client.describe_asset_model(assetModelId=asset_model_id)
        state = response["assetModelStatus"]["state"]
        if state in ('ACTIVE', 'FAILED'):
            complete_status = True
            break
        time.sleep(5)
    return complete_status

def describe_asset(asset_id):
    response = client.describe_asset(
    assetId=asset_id,
    excludeProperties=True
    )
    return response

def describe_asset_model(asset_model_id):
    response = client.describe_asset_model(
    assetModelId=asset_model_id
    )
    return response

def list_associated_assets(asset_id, hierarchy_id):
    response = client.list_associated_assets(
    assetId=asset_id,
    hierarchyId=hierarchy_id
    )
    return response

def disassociate_assets(asset_id, hierarchy_id, child_asset_id):
    response = client.disassociate_assets(
    assetId=asset_id,
    hierarchyId=hierarchy_id,
    childAssetId=child_asset_id
    )
    return response

def disassociate_all_assets(asset_id):
    asset_ids_to_delete.append(asset_id)
    describe_asset_res = describe_asset(asset_id)
    asset_name = describe_asset_res["assetName"]
    print(f'\tAsset: {asset_name}')
    asset_hierarchies = describe_asset_res["assetHierarchies"]
    child_asset_ids = []
    for asset_hierarchy in asset_hierarchies:
        asset_hierarchy_id = asset_hierarchy["id"]
        asset_summaries = list_associated_assets(asset_id, asset_hierarchy_id)["assetSummaries"]
        for asset in asset_summaries:
            child_asset_ids.append(asset["id"])
            disassociate_assets(asset_id, asset_hierarchy_id, asset["id"]) 
            print(f'\t\tChild: {asset["name"]}')
    if len(child_asset_ids) == 0: print(f'\t\tNo child assets found, skip')
    for child_asset_id in child_asset_ids:
        disassociate_all_assets(child_asset_id)

def remove_properties_hierarchies_from_all_models(asset_model_id):
    asset_model_ids_to_delete.append(asset_model_id)
    describe_asset_model_res = describe_asset_model(asset_model_id)
    asset_model_name = describe_asset_model_res["assetModelName"]
    asset_model_hierarchies = describe_asset_model_res["assetModelHierarchies"]
    
    child_asset_model_ids = []
    for asset_model_hierarchy in asset_model_hierarchies:
        child_asset_model_ids.append(asset_model_hierarchy["childAssetModelId"])

    # Remove composite models such as L4E definitions
    composite_model_summaries = describe_asset_model_res["assetModelCompositeModelSummaries"]
    for composite_model in composite_model_summaries:
        client.delete_asset_model_composite_model(assetModelId=asset_model_id, 
            assetModelCompositeModelId=composite_model["id"])
        confirm_model_update_complete(asset_model_id)

    client.update_asset_model(
    assetModelId=asset_model_id,
    assetModelName=asset_model_name,
    assetModelProperties=[],
    assetModelHierarchies=[],
    assetModelCompositeModels=[])
    if confirm_model_update_complete(asset_model_id):
        print(f'\tUpdated model: {asset_model_name}')
        for child_asset_model_id in child_asset_model_ids:
            remove_properties_hierarchies_from_all_models(child_asset_model_id)

def delete_assets(asset_ids):
    asset_ids = list(set(asset_ids)) # remove any duplicates
    for asset_id in asset_ids:
        describe_asset_res = describe_asset(asset_id)
        asset_name = describe_asset_res["assetName"]
        delete_asset(asset_id)
        print(f'\tRemoved asset: {asset_name}')

def delete_asset_models(asset_model_ids):
    asset_model_ids = list(set(asset_model_ids)) # remove any duplicates
    for asset_model_id in asset_model_ids:
        describe_asset_model_res = describe_asset_model(asset_model_id)
        asset_model_name = describe_asset_model_res["assetModelName"]
        delete_asset_model(asset_model_id)
        print(f'\tRemoved asset model: {asset_model_name}')

def delete_asset(asset_id):
    client.delete_asset(assetId=asset_id)

def confirm_assets_do_not_exist(asset_ids, asset_model_id):
    assets_do_not_exist = False
    while True:  
        res = client.list_assets(assetModelId=asset_model_id)
        for asset in res["assetSummaries"]:
            if asset["id"] in asset_ids:
                time.sleep(5)
                continue
        assets_do_not_exist = True
        break
    return assets_do_not_exist

def delete_asset_model(asset_model_id):
    confirm_assets_do_not_exist(asset_ids_to_delete, asset_model_id)
    client.delete_asset_model(assetModelId=asset_model_id) 

if __name__ == "__main__":
    # Get argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset-external-id", action="store", required=True)
    args = parser.parse_args()

    ## Models and Assets
    asset_external_id = args.asset_external_id
    asset_id = internal_from_external_asset_id(asset_external_id)
    if asset_id:
        print(f'\nDisassociating assets..')
        disassociate_all_assets(asset_id)
        print(f'\nRemoving properties and hierarchies from models..')
        asset_model_id = describe_asset(asset_id)["assetModelId"]
        remove_properties_hierarchies_from_all_models(asset_model_id)
        print(f'\nRemoving assets..')
        delete_assets(asset_ids_to_delete)
        print(f'\nRemoving asset models..')
        delete_asset_models(asset_model_ids_to_delete)
    else:
        print(f'\nAsset does not exist, no asset models and assets to remove')

    print(f'\nScript executed successfully!\n')