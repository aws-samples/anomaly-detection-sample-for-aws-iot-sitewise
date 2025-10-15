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
python3 src/data-simulation/import_historical_data.py \
    --data-file-name <file_name>
'''

# Example:
'''
python3 src/data-simulation/import_historical_data.py \
  --data-file-name historical_data_sample.csv
'''

import pandas as pd
import time
import argparse
import logging
import yaml
import boto3
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

LABELS_FILE_NAME = "labels_sample.csv"
DATA_COLUMN_NAMES = ["TIMESTAMP_SECONDS", "ALIAS", "VALUE", "DATA_TYPE", "TIMESTAMP_NANO_OFFSET", "QUALITY"]

class HistoricalDataImporter:
    def __init__(self):
        """Initialize the data importer with AWS clients and configuration"""
        self.sitewise = boto3.client('iotsitewise')
        self.s3 = boto3.client('s3')
        self.config = self._load_config()
        self.data_dir = Path(__file__).parent.absolute()
        
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
    
    def transform_data(self, df: pd.DataFrame) -> str:
        """Transform input dataframe to SiteWise format"""
        # Melt the DataFrame
        result_df = pd.melt(
            df,
            id_vars=['time_seconds'],
            value_vars=[col for col in df.columns if col != 'time_seconds'],
            var_name='ALIAS',
            value_name='VALUE'
        )
        
        # Update to match SiteWise schema
        result_df = result_df.rename(columns={'time_seconds': 'TIMESTAMP_SECONDS'})
        result_df['DATA_TYPE'] = 'DOUBLE'
        result_df['TIMESTAMP_NANO_OFFSET'] = 0
        result_df['QUALITY'] = 'GOOD'

        output_path = self.data_dir / "ebike_data_historical_30_days_transformed.csv"
        result_df.to_csv(output_path, index=False)
        logger.info(f"Transformed data saved at {output_path}")
        
        return str(output_path)

    def upload_to_s3(self, file_path: str, s3_key: str, s3_bucket_name: str) -> None:
        """Upload file to S3"""
        try:
            self.s3.upload_file(file_path, s3_bucket_name, s3_key)
            logger.info(f'Uploaded data file to S3 at s3://{s3_bucket_name}/{s3_key}')
        except Exception as e:
            logger.error(f'Failed to upload file to S3: {e}')
            raise
    
    def create_import_job(self, s3_key: str) -> str:
        """Create SiteWise bulk import job"""
        bucket = self.config["data_import"]["s3_bucket_name"]
        role_arn = self.config["data_import"]["role_arn"]
        
        try:
            response = self.sitewise.create_bulk_import_job(
                jobName=f'job_{int(time.time())}',
                jobRoleArn=role_arn,
                files=[{'bucket': bucket, 'key': s3_key}],
                errorReportLocation={
                    'bucket': bucket,
                    'prefix': self.config["data_import"]["error_prefix"]
                },
                jobConfiguration={
                    'fileFormat': {
                        'csv': {'columnNames': DATA_COLUMN_NAMES}
                    }
                }
            )
            job_id = response['jobId']
            logger.info(f'Created SiteWise bulk import job: {job_id}')
            return job_id
        except Exception as e:
            logger.error(f"Failed to create import job: {e}")
            raise
    
    def monitor_job(self, job_id: str) -> None:
        """Monitor job status"""
        sleep_secs = 10
        logger.info(f'Checking status for job {job_id} every {sleep_secs} seconds')

        while True:
            try:
                response = self.sitewise.describe_bulk_import_job(jobId=job_id)
                status = response.get("jobStatus")
                
                if status in ['PENDING', 'RUNNING']:
                    logger.info(f'\tStatus: {status}')
                elif status == 'COMPLETED':
                    logger.info('Job execution successfully completed')
                    return
                elif status == 'COMPLETED_WITH_FAILURES':
                    logger.warning('Job execution completed with failures')
                    return
                elif status == 'FAILED':
                    logger.error('Job execution failed')
                    raise Exception(f'Job failed: {job_id}')
                
                time.sleep(sleep_secs)
            except Exception as e:
                logger.error(f"Error monitoring job: {e}")
                raise
    
    def import_data(self, data_file_name: str) -> None:
        """Import historical data to SiteWise"""
        try:
            # Read input data
            file_path = self.data_dir / data_file_name
            df = pd.read_csv(file_path)
            if df.empty:
                raise ValueError("Provided file has no data")
            logger.info(f"Found {len(df)} rows of data in the file")
            
            # Transform data
            logger.info("\nPreparing data file for SiteWise import")
            transformed_path = self.transform_data(df)
            
            # Upload historical data to S3
            s3_key = f'{self.config["data_import"]["data_prefix"]}{Path(transformed_path).name}'
            logger.info("\nUploading data file to S3")
            self.upload_to_s3(transformed_path, s3_key, self.config["data_import"]["s3_bucket_name"])

            # Upload labels to S3 if labels file exists
            labels_file_path = self.data_dir / LABELS_FILE_NAME
            if labels_file_path.exists():
                labels_s3_key = f'{self.config["anomaly_detection"]["training"]["labels"]["prefix"]}{Path(labels_file_path).name}'
                logger.info("\nUploading labels file to S3")
                self.upload_to_s3(labels_file_path, labels_s3_key, self.config["anomaly_detection"]["training"]["labels"]["bucket_name"])
            
            # Create and monitor import job
            logger.info("\nCreating SiteWise bulk import job")
            job_id = self.create_import_job(s3_key)
            self.monitor_job(job_id)
            
            # Clean up
            try:
                Path(transformed_path).unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Failed to remove temporary file: {e}")
                
            logger.info("\nImport completed successfully")
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Import historical data to IoT SiteWise")
    parser.add_argument("--data-file-name", required=True, help="Name of data file")
    args = parser.parse_args()
    
    importer = HistoricalDataImporter()
    importer.import_data(args.data_file_name)

if __name__ == "__main__":
    main()
