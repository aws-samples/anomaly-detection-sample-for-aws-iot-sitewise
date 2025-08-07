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
python3 src/anomaly-detection/train_model.py
'''

# Examples:
'''
python3 src/anomaly-detection/train_model.py
'''

import yaml
import json
import time
import logging
from pathlib import Path
from typing import List, Dict
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

class AnomalyModelTrainer:
    def __init__(self, config_path=None):
        self.sitewise = boto3.client('iotsitewise')
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path=None):
        """Load configuration from YAML file"""
        if not config_path:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / 'config' / 'project_config.yml'
            
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
            raise
    
    def get_computation_model_state(self, computation_model_id: str) -> str:
        """Get computation model state"""
        response = self.sitewise.describe_computation_model(computationModelId=computation_model_id)
        return response["computationModelStatus"]["state"]
    
    def wait_for_computation_model_creation(self, computation_model_id: str) -> None:
        """Wait for computation model to become active"""
        logger.info('Checking model state every 5 seconds')
        while True:
            model_state = self.get_computation_model_state(computation_model_id)
            if model_state == "FAILED":
                raise Exception('Model creation failed')
            elif model_state == "ACTIVE":
                logger.info('Model is active')
                break
            logger.info('Still creating the model')
            time.sleep(5)
    
    def create_computation_model(self, asset_id: str, input_property_ids: List[str], 
                                result_property_id: str, model_name: str) -> str:
        """Create anomaly detection computation model"""
        try:
            # Create input properties payload
            input_properties = [{"assetProperty": {"assetId": asset_id, "propertyId": prop_id}} 
                               for prop_id in input_property_ids]
            
            # Create computation model
            response = self.sitewise.create_computation_model(
                computationModelName=model_name,
                computationModelConfiguration={
                    "anomalyDetection": {
                        "inputProperties": "${input_properties}",
                        "resultProperty": "${result_property}"
                    }
                },
                computationModelDataBinding={
                    "input_properties": {"list": input_properties},
                    "result_property": {
                        "assetProperty": {
                            "assetId": asset_id,
                            "propertyId": result_property_id
                        }
                    }
                }
            )
            
            computation_model_id = response["computationModelId"]
            logger.info(f'Created computation model id: {computation_model_id}')
            self.wait_for_computation_model_creation(computation_model_id)
            return computation_model_id
        except Exception as e:
            logger.error(f"Failed to create computation model: {e}")
            raise
    
    def get_action_definition_id(self, computation_model_id: str, action_type: str) -> str:
        """Get action definition ID for a computation model"""
        response = self.sitewise.describe_computation_model(computationModelId=computation_model_id)
        
        for action_def in response.get("actionDefinitions", []):
            if action_def.get("actionName") == action_type:
                return action_def.get("actionDefinitionId")
        
        logger.warning(f"No action definition found for action type '{action_type}'")
        return None
    
    def train_model(self, computation_model_id: str, training_config: Dict) -> str:
        """Train anomaly detection model"""
        action_definition_id = self.get_action_definition_id(
            computation_model_id, "AWS/ANOMALY_DETECTION_TRAINING")
        logger.info(f'Got action definition Id: {action_definition_id}')
        
        # Build action payload
        action_payload = {
            "exportDataStartTime": training_config["data_start_time"],
            "exportDataEndTime": training_config["data_end_time"],
            "targetSamplingRate": training_config["target_sampling_rate"]
        }
        
        # Add evaluation if provided
        if training_config["evaluation"]["bucket_name"]:
            action_payload["modelEvaluationConfiguration"] = {
                "dataStartTime": training_config["evaluation"]["data_start_time"],
                "dataEndTime": training_config["evaluation"]["data_end_time"],
                "resultDestination": {
                    "bucketName": training_config["evaluation"]["bucket_name"],
                    "prefix": training_config["evaluation"]["prefix"]
                }
            }
            logger.info("Starting to train with evaluation")
        else:
            logger.info("Starting to train without evaluation")
        
        # Add labels if provided
        if training_config["labels"]["bucket_name"]:
            action_payload["labelInputConfiguration"] = {
                "bucketName": training_config["labels"]["bucket_name"],
                "prefix": training_config["labels"]["prefix"]
            }
            logger.info("Starting to train with labels")
        else:
            logger.info("Starting to train without labels")
        
        # Execute training action
        response = self.sitewise.execute_action(
            actionDefinitionId=action_definition_id,
            actionPayload={"stringValue": json.dumps(action_payload)},
            targetResource={"computationModelId": computation_model_id}
        )
        
        action_id = response["actionId"]
        logger.info(f'Submitted training action with Action Id: {action_id}')
        return action_id
    
    def run(self):
        """Run the full training process"""
        ad_config = self.config["anomaly_detection"]
        
        # Create computation model
        computation_model_id = self.create_computation_model(
            ad_config["asset_id"],
            ad_config["input_property_ids"],
            ad_config["result_property_id"],
            ad_config["computation_model_name"]
        )
        
        # Train model
        self.train_model(computation_model_id, ad_config["training"])
        
if __name__ == "__main__":
    trainer = AnomalyModelTrainer()
    trainer.run()
    logger.info('Script execution completed successfully')