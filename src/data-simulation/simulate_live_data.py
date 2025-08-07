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
python3 src/data-simulation/simulate_live_data.py \
    --data-file-name <file_name>
'''

# Example:
'''
nohup python3 src/data-simulation/simulate_live_data.py \
  --data-file-name historical_data_sample.csv \
    >> src/data-simulation/data_simulation.log 2>&1 &
'''

import pandas as pd
import time
import argparse
import logging
from typing import List
import boto3
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

class DataSimulator:
    def __init__(self, data_file: str):
        """Initialize the data simulator"""
        self.sitewise = boto3.client('iotsitewise')
        self.data_dir = Path(__file__).parent.absolute()
        self.df = self._load_data(data_file)
        self.aliases = self.df.columns.tolist()[1:]  # Exclude time_seconds column
        
    def _load_data(self, data_file: str) -> pd.DataFrame:
        """Load data from CSV file"""
        file_path = self.data_dir / data_file
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                raise ValueError("Provided file has no data")
                
            logger.info(f"Found {len(df)} rows of data in the file")
            return df
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            raise
    
    def _send_batch(self, timestamp: int, values: List[float], aliases: List[str], 
                   batch_size: int = 10) -> None:
        """Send data to SiteWise in batches"""
        for i in range(0, len(values), batch_size):
            try:
                entries = []
                batch_values = values[i:i + batch_size]
                batch_aliases = aliases[i:i + batch_size]
                
                for idx, alias in enumerate(batch_aliases):
                    entries.append({
                        'entryId': str(idx),
                        'propertyAlias': alias,
                        'propertyValues': [{
                            'value': {'doubleValue': batch_values[idx]},
                            'timestamp': {'timeInSeconds': timestamp},
                            'quality': 'GOOD'
                        }]
                    })
                    
                if entries:
                    self.sitewise.batch_put_asset_property_value(entries=entries)
            except Exception as e:
                logger.error(f"Error processing batch starting at index {i}: {e}")
                raise
    
    def run(self, interval: int = 5) -> None:
        """Run the simulation"""
        logger.info(f"Simulating and publishing data into SiteWise every {interval} seconds")
        row_count = len(self.df)
        current_index = 0
        
        try:
            while True:
                # Get current row values (excluding time_seconds)
                row = self.df.iloc[current_index]
                values = row.values.tolist()[1:]
                
                # Send data with current timestamp
                timestamp = int(time.time())
                self._send_batch(timestamp, values, self.aliases)
                logger.info(f"{timestamp} - published data")
                
                # Move to next row (loop back to start when reaching the end)
                current_index = (current_index + 1) % row_count
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("\nStopping simulation")
        except Exception as e:
            logger.error(f"Error during simulation: {e}")
            raise

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Simulate live data for IoT SiteWise")
    parser.add_argument("--data-file-name", required=True, help="Name of data file")
    args = parser.parse_args()
    
    simulator = DataSimulator(args.data_file_name)
    simulator.run()
    
    logger.info("Script execution completed successfully")

if __name__ == "__main__":
    main()