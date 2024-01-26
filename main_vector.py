#!/usr/bin/env python3
from utils import S3ObjectMonitor, Report
from utils.vector_pipeline import AddData, DeleteData
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
_STATE_FILE = "lwi_buckets_state.json"
_LAST_RUN = "lwi_last_run.json"
_FILE_TYPE = "shp"

if __name__ == "__main__":
    # Create an instance of the S3ObjectMonitor class
    monitor = S3ObjectMonitor(_PROJECT, _BUCKETS, _PATH, _STATE_FILE, _LAST_RUN,_FILE_TYPE)

    # Monitor objects and perform the comparison
    modified_objects = monitor.monitor_objects()
    new_elements = modified_objects["added"]
    old_elements = modified_objects["removed"]
    if new_elements or old_elements:

        #Deleting old elements    
        for removed in old_elements:
            item_deleted = DeleteData(path=removed)
            item_deleted.execute()
            """regions = item_deleted.get_regions()
            storm_id = item_deleted.get_storm_id()
            for r in regions:
                report = Report(r, storm_id)
                report.delete()"""

        #Processing new elements
        for added in new_elements:
            item_to_add = AddData(path=added)
            item_to_add.execute()
            regions = item_to_add.get_regions()
            storm_id = item_to_add.get_storm_id()
            for r in regions:
                report = Report(r, storm_id)
                report.generate()
            
        
    else:
        logging.info("No new elements to process")
