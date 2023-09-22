#!/usr/bin/env python3
from utils import S3ObjectMonitor
from utils.vector_pipeline import AddData
from utils import database_utils as db
import os

os.environ['AWS_PROFILE'] = 'LWI'
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
    
    #Processing new elements
    for added in modified_objects["added"]:
        item_to_add = AddData(path=added)
        item_to_add.execute()