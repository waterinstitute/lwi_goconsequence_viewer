import unittest
from unittest.mock import patch
from moto import mock_s3
import boto3
import logging

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from main_raster import main


class TestMainRaster(unittest.TestCase):
    @mock_s3
    def setUp(self):
        # Set up the mock S3 environment
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "lwi-common"
        self.s3.create_bucket(Bucket=self.bucket_name)
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key="lwi-region0/lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth.tif",
            Body=b"test data",
        )

    @mock_s3
    @patch("main_raster.S3ObjectMonitor")
    def test_monitor_objects(self, MockS3ObjectMonitor):
        # Mock the S3ObjectMonitor
        mock_monitor = MockS3ObjectMonitor.return_value
        mock_monitor.monitor_objects.return_value = {
            "added": [
                {
                    "Bucket": "lwi-common",
                    "Key": "lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth.tif",
                }
            ],
            "removed": [],
        }

        # Run the main function
        main()

        # Check if the logging was called with the expected messages
        with self.assertLogs(level="INFO") as log:
            logging.getLogger().info(
                "New elements: [{'Bucket': 'lwi-common', 'Key': 'lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth.tif'}]"
            )
            logging.getLogger().info("Removed elements: []")
            self.assertIn(
                "INFO:root:New elements: [{'Bucket': 'lwi-common', 'Key': 'lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth.tif'}]",
                log.output,
            )
            self.assertIn("INFO:root:Removed elements: []", log.output)


if __name__ == "__main__":
    unittest.main()
