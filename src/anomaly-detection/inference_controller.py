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
python3 src/anomaly-detection/inference_controller.py \
  --computation-model-id <value> \
  --mode <value>
'''

# Examples:
'''
python3 src/anomaly-detection/inference_controller.py \
  --computation-model-id 7f828d49-86ba-4c5b-aca1-4f575cb5648d \
  --mode START

python3 src/anomaly-detection/inference_controller.py \
  --computation-model-id 72f2a667-f869-4302-aed2-ac2a1b2bf9b0 \
  --mode STOP
'''

import yaml
import json
import time
import logging
from pathlib import Path
import boto3
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

INFERENCE_ACTION_TYPE = "AWS/ANOMALY_DETECTION_INFERENCE"

class InferenceController:
    def __init__(self, config_path=None):
        """Initialize the inference controller with configuration"""
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
    
    def get_action_definition_id(self, computation_model_id: str, action_type: str) -> str:
        """Get action definition ID for a computation model"""
        try:
            response = self.sitewise.describe_computation_model(
                computationModelId=computation_model_id
            )
            
            for action_def in response.get("actionDefinitions", []):
                if action_def.get("actionName") == action_type:
                    return action_def.get("actionDefinitionId")
            
            logger.warning(f"No action definition found for action type '{action_type}'")
            return None
        except Exception as e:
            logger.error(f"Error getting action definition: {e}")
            raise
    
    def execute_inference_action(self, computation_model_id: str, mode: str) -> str:
        """Execute inference action (START or STOP)"""
        try:
            action_definition_id = self.get_action_definition_id(
                computation_model_id, INFERENCE_ACTION_TYPE)
                
            if not action_definition_id:
                raise ValueError("Failed to get action definition ID for inference")
            
            # Build action payload
            action_payload = {"inferenceMode": mode}
            
            if mode == "START":
                inference_config = self.config["anomaly_detection"]["inference"]
                action_payload["dataDelayOffsetInMinutes"] = inference_config["data_delay_offset_minutes"]
                action_payload["dataUploadFrequency"] = inference_config["data_upload_frequency"]
                
                if inference_config["weekly_operating_window"]:
                    action_payload["weeklyOperatingWindow"] = inference_config["weekly_operating_window"]
            
            # Execute action
            response = self.sitewise.execute_action(
                actionDefinitionId=action_definition_id,
                actionPayload={"stringValue": json.dumps(action_payload)},
                targetResource={"computationModelId": computation_model_id}
            )
            
            action_id = response["actionId"]
            logger.info(f'Inference {mode} mode executed with action ID: {action_id}')
            return action_id
        except Exception as e:
            logger.error(f"Failed to execute inference action: {e}")
            raise
    
    def wait_for_completion(self, computation_model_id: str, mode: str) -> None:
        """Wait for inference action to complete"""
        logger.info('Checking inference status every 5 seconds')
        
        while True:
            response = self.sitewise.describe_computation_model_execution_summary(
                computationModelId=computation_model_id
            )
            
            computation_summary = response["computationModelExecutionSummary"]
            inference_active = computation_summary["inferenceTimerActive"] == "true"
            
            if mode == "START" and inference_active:
                logger.info(f'Inference started for computation model: {computation_model_id}')
                break
            elif mode == "STOP" and not inference_active:
                logger.info(f'Inference stopped for computation model: {computation_model_id}')
                break
            time.sleep(5)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Control IoT SiteWise anomaly detection inference"
    )
    parser.add_argument("--computation-model-id", required=True, help="Computation Model ID")
    parser.add_argument("--mode", required=True, choices=["START", "STOP"], 
                      help="Inference action mode (START or STOP)")
    return parser.parse_args()

def main():
    """Main entry point for the script"""
    args = parse_args()
    
    logger.info(f'\nInference action requested: {args.mode}')
    
    controller = InferenceController()
    controller.execute_inference_action(args.computation_model_id, args.mode)
    
    # Add delay for action to propagate
    time.sleep(10)
    
    # Verify inference state change
    controller.wait_for_completion(args.computation_model_id, args.mode)
    
    logger.info('\nScript execution completed successfully')

if __name__ == "__main__":
    main()