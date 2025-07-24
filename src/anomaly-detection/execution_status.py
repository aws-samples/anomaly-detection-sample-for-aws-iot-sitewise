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
python3 src/anomaly-detection/execution_status.py \
  --action <value> \
  [--computation-model-id <value>] \
  [--asset-id <value>]
'''

# Examples:
'''
python3 src/anomaly-detection/execution_status.py \
  --action TRAINING \
  --computation-model-id b9b8828e-26e2-4e48-9744-9dbdc5b3702a

python3 src/anomaly-detection/execution_status.py \
  --action INFERENCE \
  --asset-id 9487f2bd-2c9c-48ce-a958-909e38490b97

python3 src/anomaly-detection/execution_status.py \
  --action TRAINING \
  --asset-id 9487f2bd-2c9c-48ce-a958-909e38490b97
'''

import logging
import argparse
from typing import List, Dict, Tuple
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)
logger = logging.getLogger(__name__)

class ExecutionStatusChecker:
    def __init__(self):
        self.sitewise = boto3.client('iotsitewise')
    
    def get_execution_history(self, computation_model_id: str, action: str) -> List[Dict]:
        """Get execution history for a computation model"""
        execution_summaries = []
        try:
            next_token = None
            
            while True:
                # Prepare request parameters
                params = {
                    "targetResourceType": "COMPUTATION_MODEL",
                    "targetResourceId": computation_model_id,
                    "actionType": f"AWS/ANOMALY_DETECTION_{action.upper()}"
                }
                if next_token:
                    params["nextToken"] = next_token
                
                # Make API call with pagination
                response = self.sitewise.list_executions(**params)
                
                # Collect results from current page
                execution_summaries.extend(response.get("executionSummaries", []))
                
                # Check if there are more pages
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
            result_property_key = response["computationModelConfiguration"]["anomalyDetection"]["resultProperty"].strip("${}")
            return response["computationModelDataBinding"][result_property_key]["assetProperty"]["assetId"]
        except Exception as e:
            logger.error(f"Failed to find asset ID for computation model {computation_model_id}: {e}")
            raise

    def get_computation_models_for_asset(self, asset_id: str) -> List[str]:
        """Get computation model IDs associated with the asset"""
        computation_model_ids = []
        try:
            next_token = None
            
            while True:
                # Prepare request parameters
                params = {"computationModelType": "ANOMALY_DETECTION"}
                if next_token:
                    params["nextToken"] = next_token
                
                # Make API call with pagination
                response = self.sitewise.list_computation_models(**params)
                
                # Process current page of results
                for summary in response.get("computationModelSummaries", []):
                    model_id = summary["id"]
                    try:
                        if asset_id == self.get_asset_id_for_computation_model(model_id):
                            computation_model_ids.append(model_id)
                    except Exception:
                        continue
                
                # Check if there are more pages
                next_token = response.get("nextToken")
                if not next_token:
                    break
            
            return computation_model_ids
        except Exception as e:
            logger.error(f"Failed to get computation models for asset {asset_id}: {e}")
            raise

    def check_execution_status(self, computation_model_id: str, action: str) -> None:
        """Check and display execution status"""
        logger.info(f"\nLooking for {action.lower()} executions for computation model {computation_model_id}")
        executions = self.get_execution_history(computation_model_id, action)
        
        if not executions: 
            logger.info("Found no executions")
            return
            
        logger.info(f"Found {len(executions)} executions")
        latest = executions[0]
        
        # Enrich execution data
        latest["executionResultMessage"] = self._get_execution_result_message(latest.get("executionId"))
        
        if action == "INFERENCE" and latest.get("executionStatus", {}).get("state") != "FAILED":
            latest["anomalyResult"] = self._get_latest_inference_result(computation_model_id)
            
        self._display_execution(latest, action)
    
    def _get_execution_result_message(self, execution_id: str) -> str:
        """Get execution result message"""
        try:
            response = self.sitewise.describe_execution(executionId=execution_id)
            return response.get("executionResult", {}).get("message")
        except Exception as e:
            logger.error(f"Error getting execution result: {e}")
            return "Error retrieving result message"
                                                                   
    def _display_execution(self, execution: Dict, action: str) -> None:
        """Display execution details"""
        logger.info(f"Details of the latest {action.lower()} execution:")
        print(f"\tExecution ID: {execution.get('executionId')}")
        print(f"\tStatus: {execution.get('executionStatus', {}).get('state')}")
        print(f"\tStart time: {self._format_timestamp(execution.get('executionStartTime'))}")
        print(f"\tEnd time: {self._format_timestamp(execution.get('executionEndTime'))}")
        print(f"\tExecution Message: {execution.get('executionResultMessage')}")
        
        if action == "INFERENCE" and execution.get("anomalyResult"):
            print(f"\tAnomaly Result: {execution.get('anomalyResult')}")

    def _get_latest_inference_result(self, computation_model_id: str) -> str:
        """Get latest inference result"""
        try:
            asset_id, property_id = self._get_result_property_ids(computation_model_id)
            response = self.sitewise.get_asset_property_value(
                assetId=asset_id, 
                propertyId=property_id
            )
            return response["propertyValue"]["value"]["stringValue"]
        except Exception as e:
            logger.error(f"Error getting inference result: {e}")
            return "Error retrieving result"

    def _get_result_property_ids(self, computation_model_id: str) -> Tuple[str, str]:
        """Get asset and property IDs for result property"""
        response = self.sitewise.describe_computation_model(computationModelId=computation_model_id)
        result_key = response["computationModelConfiguration"]["anomalyDetection"]["resultProperty"].strip("${}")
        asset_property = response["computationModelDataBinding"][result_key]["assetProperty"]
        return asset_property["assetId"], asset_property["propertyId"]
    
    @staticmethod
    def _format_timestamp(timestamp_obj) -> str:
        """Format timestamp or return N/A if None"""
        return timestamp_obj.strftime("%B %d, %Y %I:%M:%S %p %Z") if timestamp_obj else "N/A"

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Check IoT SiteWise anomaly detection execution status")
    parser.add_argument('--action', required=True, choices=['TRAINING', 'INFERENCE'], 
                        help='Action type to check status for')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--computation-model-id', help='Computation Model ID')
    group.add_argument('--asset-id', help='Asset ID')
    
    args = parser.parse_args()
    checker = ExecutionStatusChecker()
    
    if args.asset_id:
        model_ids = checker.get_computation_models_for_asset(args.asset_id)
        if not model_ids:
            logger.error(f"\nNo computation models found for asset {args.asset_id}")
            exit(1)
        logger.info(f"\nFound {len(model_ids)} computation models for asset {args.asset_id}")
    else:
        model_ids = [args.computation_model_id]
    
    for model_id in model_ids:
        checker.check_execution_status(model_id, args.action)

if __name__ == "__main__":
    main()