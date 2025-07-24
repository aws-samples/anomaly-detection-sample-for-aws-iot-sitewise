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
python3 src/clean-up/remove_resources.py \
  --asset-external-id <value>
'''

# Examples:
'''
python3 src/clean-up/remove_resources.py \
  --asset-external-id Workshop_Corporate_AnyCompany_AD
'''

import boto3
import time
import logging
import yaml
import psutil
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

# Constants
DATA_SIMULATION_SCRIPT_NAME = "simulate_live_data.py"
DATA_STREAM_PREFIX = "/Tag Providers/AD/default/UR"

class ResourceCleaner:
    def __init__(self):
        self.sitewise = boto3.client('iotsitewise')
        self.asset_model_ids_to_delete = []
        self.asset_ids_to_delete = []
        self.config = self._load_config()
        
    def _load_config(self):
        """Load project configuration"""
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / 'config' / 'project_config.yml'
        
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def describe_asset(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Get asset details"""
        try:
            return self.sitewise.describe_asset(assetId=asset_id, excludeProperties=True)
        except self.sitewise.exceptions.ResourceNotFoundException:
            logger.warning(f"Asset {asset_id} not found")
            return None
        except Exception as e:
            logger.error(f"Error describing asset {asset_id}: {e}")
            raise
    
    def describe_asset_model(self, asset_model_id: str) -> Optional[Dict[str, Any]]:
        """Get asset model details"""
        try:
            return self.sitewise.describe_asset_model(assetModelId=asset_model_id)
        except self.sitewise.exceptions.ResourceNotFoundException:
            logger.warning(f"Asset model {asset_model_id} not found")
            return None
        except Exception as e:
            logger.error(f"Error describing asset model {asset_model_id}: {e}")
            raise
    
    def wait_for_model_update(self, asset_model_id: str) -> bool:
        """Wait for asset model update to complete"""
        time.sleep(2)  # Initial delay
        
        while True:
            response = self.describe_asset_model(asset_model_id)
            state = response.get("assetModelStatus", {}).get("state", "UNKNOWN")
            
            if state == "ACTIVE":
                return True
            if state == "FAILED":
                raise Exception("Asset model update failed")
            
            time.sleep(5)  # Poll interval
    
    def disassociate_all_assets(self, asset_id: str) -> None:
        """Recursively disassociate all child assets"""
        self.asset_ids_to_delete.append(asset_id)
        
        try:
            asset_details = self.describe_asset(asset_id)
            if not asset_details:
                return
                
            asset_name = asset_details["assetName"]
            logger.info(f'\tAsset: {asset_name}')
            
            child_assets = []
            for hierarchy in asset_details.get("assetHierarchies", []):
                hierarchy_id = hierarchy["id"]
                
                try:
                    response = self.sitewise.list_associated_assets(
                        assetId=asset_id, 
                        hierarchyId=hierarchy_id
                    )
                    
                    for asset in response.get("assetSummaries", []):
                        child_id = asset["id"]
                        child_name = asset["name"]
                        child_assets.append((child_id, child_name))
                        
                        self.sitewise.disassociate_assets(
                            assetId=asset_id,
                            hierarchyId=hierarchy_id,
                            childAssetId=child_id
                        )
                        logger.info(f'\t\tChild: {child_name}')
                except Exception as e:
                    logger.error(f"Error processing hierarchy {hierarchy_id}: {e}")
            
            # Recursively process children
            for child_id, _ in child_assets:
                self.disassociate_all_assets(child_id)
                
        except Exception as e:
            logger.error(f"Error disassociating assets for {asset_id}: {e}")
    
    def remove_model_properties(self, asset_model_id: str) -> None:
        """Recursively remove properties and hierarchies from asset models"""
        try:
            model_details = self.describe_asset_model(asset_model_id)
            if not model_details:
                return
                
            asset_model_name = model_details["assetModelName"]
            
            # Get child models before updating
            child_model_ids = [
                hierarchy["childAssetModelId"] 
                for hierarchy in model_details.get("assetModelHierarchies", [])
            ]
            
            # Track for deletion
            if asset_model_id not in self.asset_model_ids_to_delete:
                self.asset_model_ids_to_delete.append(asset_model_id)
            
            # Update model to remove properties and hierarchies
            self.sitewise.update_asset_model(
                assetModelId=asset_model_id,
                assetModelName=asset_model_name,
                assetModelProperties=[],
                assetModelHierarchies=[],
                assetModelCompositeModels=[]
            )
            
            if self.wait_for_model_update(asset_model_id):
                logger.info(f'\tUpdated model: {asset_model_name}')
                
                # Process child models
                for child_id in child_model_ids:
                    self.remove_model_properties(child_id)
                    
        except Exception as e:
            logger.error(f"Error removing properties from model {asset_model_id}: {e}")
    
    def delete_assets(self) -> None:
        """Delete assets"""
        unique_ids = list(set(self.asset_ids_to_delete))
        
        for asset_id in unique_ids:
            try:
                asset_info = self.describe_asset(asset_id)
                if not asset_info:
                    continue
                    
                asset_name = asset_info["assetName"]
                self.sitewise.delete_asset(assetId=asset_id)
                logger.info(f'\tRemoved asset: {asset_name}')
                
            except Exception as e:
                logger.error(f"Failed to delete asset {asset_id}: {e}")
    
    def delete_asset_models(self) -> None:
        """Delete asset models"""
        unique_ids = list(set(self.asset_model_ids_to_delete))
        
        for model_id in unique_ids:
            try:
                model_info = self.describe_asset_model(model_id)
                if not model_info:
                    continue
                    
                model_name = model_info["assetModelName"]
                
                # Wait for assets to be deleted
                if self._wait_for_assets_deletion(model_id):
                    self.sitewise.delete_asset_model(assetModelId=model_id)
                    logger.info(f'\tRemoved asset model: {model_name}')
                
            except Exception as e:
                logger.error(f"Failed to delete asset model {model_id}: {e}")
    
    def _wait_for_assets_deletion(self, model_id: str) -> bool:
        """Wait for assets to be deleted"""
        asset_ids_set = set(self.asset_ids_to_delete)
        retries = 12  # 1 minute total (5s * 12)
        
        for i in range(retries):
            try:
                response = self.sitewise.list_assets(assetModelId=model_id)
                existing = [a["id"] for a in response.get("assetSummaries", []) 
                           if a["id"] in asset_ids_set]
                
                if not existing:
                    return True
                    
                time.sleep(5)
            except Exception:
                return False
                
        return False
    
    def delete_computation_models(self, asset_id: str) -> None:
        """Delete computation models related to an asset"""
        try:
            model_ids = self._get_computation_models_for_asset(asset_id)

            if not model_ids:
                logger.info(f'No computation models found for asset {asset_id}, skip')
                return

            for model_id in model_ids:
                self.sitewise.delete_computation_model(computationModelId=model_id)
                logger.info(f'Removed computation model id: {model_id}')
                
        except Exception as e:
            logger.error(f"Error deleting computation models: {e}")
    
    def _get_computation_models_for_asset(self, asset_id: str) -> List[str]:
        """Get computation models for an asset"""
        models = []
        next_token = None
        
        while True:
            params = {"computationModelType": "ANOMALY_DETECTION"}
            if next_token:
                params["nextToken"] = next_token
                
            response = self.sitewise.list_computation_models(**params)
            
            for summary in response.get("computationModelSummaries", []):
                try:
                    model_id = summary["id"]
                    model_asset_id = self._get_asset_id_for_model(model_id)
                    
                    if asset_id == model_asset_id:
                        models.append(model_id)
                except Exception:
                    continue
            
            next_token = response.get("nextToken")
            if not next_token:
                break
                
        return models
    
    def _get_asset_id_for_model(self, model_id: str) -> str:
        """Get asset ID for a computation model"""
        response = self.sitewise.describe_computation_model(computationModelId=model_id)
        result_key = response["computationModelConfiguration"]["anomalyDetection"]["resultProperty"].strip("${}")
        return response["computationModelDataBinding"][result_key]["assetProperty"]["assetId"]
    
    def delete_data_streams(self) -> None:
        """Delete data streams"""
        try:
            aliases = self._get_time_series_aliases()
            count = 0
            
            for alias in aliases:
                if DATA_STREAM_PREFIX in alias:
                    self.sitewise.delete_time_series(alias=alias)
                    count += 1
            
            if count > 0:
                logger.info(f'Removed {count} data streams with prefix {DATA_STREAM_PREFIX}')
            else:
                logger.info('No data streams to remove')
                
        except Exception as e:
            logger.error(f"Error deleting data streams: {e}")
    
    def _get_time_series_aliases(self) -> List[str]:
        """Get time series aliases"""
        aliases = []
        next_token = None
        
        while True:
            params = {'maxResults': 250, 'timeSeriesType': 'DISASSOCIATED'}
            if next_token:
                params['nextToken'] = next_token
                
            response = self.sitewise.list_time_series(**params)
            aliases.extend([item["alias"] for item in response.get("TimeSeriesSummaries", [])])
            
            next_token = response.get('nextToken')
            if not next_token:
                break
                
        return aliases
    
    def kill_simulation_process(self) -> None:
        """Kill data simulation process"""
        try:
            for proc in psutil.process_iter(['pid', 'cmdline']):
                try:
                    if DATA_SIMULATION_SCRIPT_NAME in ''.join(proc.info.get("cmdline", [])):
                        pid = proc.info['pid']
                        proc.kill()
                        logger.info(f"Killed simulation process, pid: {pid}")
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
                    
            logger.info(f"No simulation process found, skipping")
        except Exception as e:
            logger.error(f"Error killing simulation process: {e}")
    
    def _get_asset_id_from_external_id(self, asset_external_id: str) -> str:
        """Get asset ID from external ID"""
        response = self.sitewise.describe_asset(
            assetId = f"externalId:{asset_external_id}", 
            excludeProperties=True)
        asset_id = response.get("assetId")
        return asset_id
    
    def cleanup(self, asset_external_id: str) -> None:
        """Main cleanup method"""
        
        # Kill simulation process
        self.kill_simulation_process()
        
        # Cleanup resources
        asset_id = self._get_asset_id_from_external_id(asset_external_id)

        logger.info('\nDisassociating assets..')
        self.disassociate_all_assets(asset_id)
        
        logger.info('\nRemoving computation models..')
        self.delete_computation_models(self.config["anomaly_detection"]["asset_id"])
        
        logger.info('\nRemoving properties and hierarchies from models..')
        asset_model_id = self.describe_asset(asset_id)["assetModelId"]
        self.remove_model_properties(asset_model_id)
        
        logger.info('\nRemoving assets..')
        self.delete_assets()
        
        logger.info('\nRemoving asset models..')
        self.delete_asset_models()
        
        logger.info('\nRemoving data streams..')
        self.delete_data_streams()
        
        logger.info('Cleanup completed successfully!')

def main():
    parser = argparse.ArgumentParser(description="Clean up IoT SiteWise resources")
    parser.add_argument("--asset-external-id", required=True, help="External Asset ID of the root asset")
    args = parser.parse_args()
    
    try:
        cleaner = ResourceCleaner()
        cleaner.cleanup(args.asset_external_id)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise

if __name__ == "__main__":
    main()