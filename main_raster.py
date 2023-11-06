#!/usr/bin/env python3
from utils import S3ObjectMonitor
from utils.raster_pipeline import AddData, DeleteData
import os
os.environ['AWS_PROFILE'] = 'LWI'

import logging
logging.basicConfig(level=logging.INFO)

log_file = 'logs.log'
file_handler = logging.FileHandler(log_file)

# Create a formatter to specify the log message format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the FileHandler to the root logger
logging.getLogger('').addHandler(file_handler)

_MODE = "DEV"
_PROJECT = "LWI"

_DEFAULT_BUCKETS = {
    "PROD":[*[f"lwi-region{x}" for x in range(1, 8)]],
    "DEV":["lwi-common"]
}
_DEFAULT_PATHS = {
    "PROD":"deliverables/consequence_modeling_results/goconsequence_results_shp/",
    "DEV":"staging/wsel_test/"
}
_BUCKETS = _DEFAULT_BUCKETS[_MODE]
_PATH = _DEFAULT_PATHS[_MODE]
_STATE_FILE = "lwi_buckets_state_tif.json"
_LAST_RUN = "lwi_last_run_tif.json"
_FILE_TYPE = ("tif","tiff")
###Temp path to store raster data
_TEMP_PATH = "temp/"
if __name__ == "__main__":
    # Create an instance of the S3ObjectMonitor class
    monitor = S3ObjectMonitor(_PROJECT, _BUCKETS, _PATH, _STATE_FILE, _LAST_RUN,_FILE_TYPE)

    # Monitor objects and perform the comparison
    modified_objects = monitor.monitor_objects()
    new_elements = modified_objects["added"]
    old_elements = modified_objects["removed"]
    print(new_elements,old_elements)
    if new_elements or old_elements:

        #Processing new elements
        for added in new_elements:
            item_to_add = AddData(path=added,temp_path=_TEMP_PATH,s3=monitor.get_s3_client())
            item_to_add.execute()
        
        #Deleting old elements    
        for removed in old_elements:
            item_deleted = DeleteData(path=removed,s3=monitor.get_s3_client())
            item_deleted.execute()
    else:
        logging.info("No new elements to process")