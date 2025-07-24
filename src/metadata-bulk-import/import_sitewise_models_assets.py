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
python3 metadata-bulk-import/import_sitewise_models_assets.py \
  --definitions-file-name <value>
'''
# Examples:
'''
python3 src/metadata-bulk-import/import_sitewise_models_assets.py \
  --definitions-file-name definitions_models_assets.json
'''

from datetime import datetime
import time
import argparse
import yaml
import logging
import boto3
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

class MetadataImporter:
    def __init__(self):
        """Initialize the metadata importer with AWS clients and configuration"""
        self.twinmaker = boto3.client('iottwinmaker')
        self.s3 = boto3.client('s3')
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
    
    def upload_file_to_s3(self, local_file_path: str, bucket: str, s3_key: str):
        """Upload a local file to S3 bucket"""
        try:
            self.s3.upload_file(local_file_path, bucket, s3_key)
            logger.info(f'Successfully uploaded file to s3://{bucket}/{s3_key}')
        except Exception as e:
            logger.error(f'Failed to upload file to S3: {e}')
            raise
    
    def create_metadata_job(self, job_id: str, s3_bucket: str, s3_key: str):
        """Create a bulk import job"""
        try:
            self.twinmaker.create_metadata_transfer_job(
                metadataTransferJobId=job_id,
                sources=[{
                    'type': 's3',
                    's3Configuration': {
                        'location': f'arn:aws:s3:::{s3_bucket}/{s3_key}'
                    }
                }],
                destination={
                    'type': 'iotsitewise'
                }    
            )
            logger.info(f'\nCreated metadata bulk job with job Id: {job_id}')
        except Exception as e:
            logger.error(f'Failed to create metadata transfer job: {e}')
            raise
    
    def monitor_job_status(self, job_id: str):
        """Monitor and print job status"""
        logger.info(f'\nChecking status of {job_id} job every 30 seconds..')
        logger.info(f'Tip: You can also check the status from AWS IoT SiteWise console')

        while True:
            try:
                res = self.twinmaker.get_metadata_transfer_job(metadataTransferJobId=job_id)
                state = res["status"]["state"]
                
                if state in ('RUNNING', 'COMPLETED'):
                    progress = res.get("progress", {})
                    logger.info(
                        f'\tStatus: {state} | '
                        f'Total: {progress.get("totalCount", 0)}, '
                        f'Succeeded: {progress.get("succeededCount", 0)}, '
                        f'Skipped: {progress.get("skippedCount", 0)}, '
                        f'Failed: {progress.get("failedCount", 0)}'
                    )
                elif state == 'ERROR':
                    report_url = res.get("reportUrl", "No report URL available")
                    logger.error(f'\tJob failed with status: {state} | Report URL: {report_url}')
                    break
                else:
                    logger.info(f'\tStatus: {state}')

                if state == 'COMPLETED':
                    logger.info(f'\n{job_id} job successfully completed!')
                    logger.info(f'Tip: You can verify the changes in AWS IoT SiteWise console')
                    break

                time.sleep(30)

            except Exception as e:
                logger.error(f'Error checking job status: {e}')
                raise
    
    def import_definitions(self, definitions_file_name: str):
        """Import definitions from file to SiteWise"""
        try:
            # Set up paths and job ID
            import_dir = Path(__file__).parent
            s3_bucket = self.config["metadata_bulk_operations"]["s3_bucket_name"]
            local_file_path = import_dir / definitions_file_name
            s3_key = f'metadata-bulk-import/{definitions_file_name}'
            job_id = f'Workshop_AD_Import_{int(datetime.now().timestamp())}'
            
            # Execute import process
            self.upload_file_to_s3(local_file_path, s3_bucket, s3_key)
            self.create_metadata_job(job_id, s3_bucket, s3_key)
            self.monitor_job_status(job_id)
            
            logger.info('\nScript execution completed successfully')
        except Exception as e:
            logger.error(f'Import failed: {e}')
            raise

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description="Import SiteWise models and assets")
    parser.add_argument("--definitions-file-name", required=True, 
                       help="Name of the definitions file")
    args = parser.parse_args()
    
    importer = MetadataImporter()
    importer.import_definitions(args.definitions_file_name)

if __name__ == "__main__":
    main()