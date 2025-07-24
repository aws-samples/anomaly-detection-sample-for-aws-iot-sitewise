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
python3 src/anomaly-detection/execution_history.py \
  [--computation-model-id <value>] \
  [--asset-id <value>] \
  --action <value>
'''

# Examples:
'''
python3 src/anomaly-detection/execution_history.py \
  --computation-model-id 72f2a667-f869-4302-aed2-ac2a1b2bf9b0 \
  --action TRAINING

python3 src/anomaly-detection/execution_history.py \
  --computation-model-id b5dfb501-6157-4772-81c5-4f3696117594 \
  --action INFERENCE

python3 src/anomaly-detection/execution_history.py \
  --asset-id 58c42075-8af2-4d63-89de-ce38627a5624 \
  --action TRAINING
'''

import logging
import argparse
from typing import List, Dict
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    #format='%(asctime)s %(levelname)s: %(message)s',
    format='%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

class SiteWiseExecutionHistory:
    def __init__(self):
        self.sitewise = boto3.client('iotsitewise')
    
    def get_execution_history(self, computation_model_id: str, action: str) -> List[Dict]:
        """Get execution history for a computation model and action type"""
        try:
            execution_summaries = []
            next_token = None
            
            while True:
                kwargs = {
                    "targetResourceType": "COMPUTATION_MODEL",
                    "targetResourceId": computation_model_id,
                    "actionType": f"AWS/ANOMALY_DETECTION_{action}"
                }
                
                if next_token:
                    kwargs["nextToken"] = next_token
                
                response = self.sitewise.list_executions(**kwargs)
                execution_summaries.extend(response.get("executionSummaries", []))
                
                next_token = response.get("nextToken")
                if not next_token:
                    break
            
            return execution_summaries
        except Exception as e:
            logger.error(f"Error retrieving execution history: {e}")
            raise
    
    def get_asset_id_for_computation_model(self, computation_model_id: str) -> str:
        """Get asset ID associated with the computation model"""
        try:
            response = self.sitewise.describe_computation_model(computationModelId=computation_model_id)
            result_property_key_name = response["computationModelConfiguration"]["anomalyDetection"]["resultProperty"].strip("${}")
            return response["computationModelDataBinding"][result_property_key_name]["assetProperty"]["assetId"]
        except Exception as e:
            logger.error(f"Failed to find asset ID for computation model {computation_model_id}: {e}")
            raise
    
    def get_computation_models_for_asset(self, asset_id: str) -> List[str]:
        """Get computation model IDs associated with the asset"""
        computation_model_ids = []
        try:
            next_token = None
            
            while True:
                kwargs = {"computationModelType": "ANOMALY_DETECTION"}
                
                if next_token:
                    kwargs["nextToken"] = next_token
                
                response = self.sitewise.list_computation_models(**kwargs)
                
                for summary in response.get("computationModelSummaries", []):
                    model_id = summary["id"]
                    if asset_id == self.get_asset_id_for_computation_model(model_id):
                        computation_model_ids.append(model_id)
                
                next_token = response.get("nextToken")
                if not next_token:
                    break
            
            return computation_model_ids
        except Exception as e:
            logger.error(f"Failed to get computation model ID for asset {asset_id}: {e}")
            raise
    
    def display_execution_history(self, computation_model_id: str, action: str) -> None:
        """Display execution history for a computation model"""
        logger.info(f"\nLooking for {action} executions for computation model {computation_model_id}")
        executions = self.get_execution_history(computation_model_id, action)
        
        if not executions:
            logger.info("No executions found")
            return
        
        logger.info(f"Found {len(executions)} executions")
        for execution in executions:
            self._print_execution(execution)
    
    def _print_execution(self, execution: Dict) -> None:
        """Print details of an execution"""
        execution_id = execution.get("executionId")
        status = execution.get("executionStatus", {}).get("state")
        start_time = self._format_timestamp(execution.get("executionStartTime"))
        end_time = self._format_timestamp(execution.get("executionEndTime"))
        
        logger.info(f"\tExecution ID: {execution_id}, Status: {status}, Start Time: {start_time}, End Time: {end_time}")
    
    @staticmethod
    def _format_timestamp(timestamp_obj) -> str:
        """Format a timestamp object or return N/A if None"""
        return timestamp_obj.strftime("%B %d, %Y %I:%M:%S %p %Z") if timestamp_obj else "N/A"

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="List IoT SiteWise anomaly detection execution history")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--computation-model-id', help='Computation Model ID associated with the asset')
    group.add_argument('--asset-id', help='Asset ID of the asset')
    
    parser.add_argument("--action", required=True, choices=["TRAINING", "INFERENCE"], 
                        help="Action type (TRAINING or INFERENCE)")
    return parser.parse_args()

def main():
    """Main entry point for the script"""
    args = parse_args()
    history = SiteWiseExecutionHistory()
    
    if args.asset_id:
        computation_model_ids = history.get_computation_models_for_asset(args.asset_id)
        if not computation_model_ids:
            logger.error(f"\nNo computation models found for asset {args.asset_id}")
            exit(1)
        logger.info(f"\nFound {len(computation_model_ids)} computation models for asset {args.asset_id}")
    else:
        computation_model_ids = [args.computation_model_id]
    
    for model_id in computation_model_ids:
        history.display_execution_history(model_id, args.action)

if __name__ == "__main__":
    main()