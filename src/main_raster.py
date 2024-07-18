#!/usr/bin/env python3

"""
This script sets up logging and environment variables for a raster data pipeline.
It monitors an S3 bucket for new raster data, processes the data, and uploads the processed data to publish
as tile services.
"""

# Add the root directory to the Python path
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import S3ObjectMonitor
from utils.raster_pipeline import AddData, DeleteData
import logging

# Set the AWS profile environment variable

os.environ["AWS_PROFILE"] = "LWI"


# Configure logging to output messages to the console at the INFO level

logging.basicConfig(level=logging.INFO)
# Define the log file name

log_file = "logs.log"
# Create a file handler to log messages to a file

file_handler = logging.FileHandler(log_file)

# Create a formatter to specify the log message format
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the FileHandler to the root logger
logging.getLogger("").addHandler(file_handler)

# Define the mode of operation (development or production)

_MODE = "PROD"
_PROJECT = "LWI"

# Define the default S3 buckets for different environments

_DEFAULT_BUCKETS = {
    "PROD": [*[f"lwi-region{x}" for x in range(1, 8)]],
    "DEV": ["lwi-common"],
}
_DEFAULT_PATHS = {
    "PROD": "deliverables/consequence_modeling_results/depth_tiff/",
    "DEV": "staging/wsel_test/",
}
_BUCKETS = _DEFAULT_BUCKETS[_MODE]
_PATH = _DEFAULT_PATHS[_MODE]
_STATE_FILE = "lwi_buckets_state_tif.json"
_LAST_RUN = "lwi_last_run_tif.json"
_FILE_TYPE = ("tif", "tiff")
###Temp path to store raster data
_TEMP_PATH = "temp/"


def main():
    """Main function to monitor raster objects in the S3 bucket and process them."""
    # Create an instance of the S3ObjectMonitor class
    monitor = S3ObjectMonitor(
        _PROJECT, _BUCKETS, _PATH, _STATE_FILE, _LAST_RUN, _FILE_TYPE
    )

    # Monitor objects and perform the comparison
    modified_objects = monitor.monitor_objects()
    new_elements = modified_objects["added"]
    old_elements = modified_objects["removed"]
    logging.info("New elements: %s", new_elements)
    logging.info("Old elements: %s", old_elements)
    if new_elements or old_elements:
        # Deleting old elements
        for removed in old_elements:
            item_deleted = DeleteData(path=removed, s3=monitor.get_s3_client())
            item_deleted.execute()
        # Processing new elements
        for added in new_elements:
            item_to_add = AddData(
                path=added, temp_path=_TEMP_PATH, s3=monitor.get_s3_client()
            )
            item_to_add.execute()

    else:
        logging.info("No new elements to process")


if __name__ == "__main__":
    main()
