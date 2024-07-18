import unittest
from unittest.mock import patch, MagicMock
from moto import mock_s3
import boto3

# Import the script to be tested
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from main_vector import main


class TestMainVector(unittest.TestCase):
    @mock_s3
    def setUp(self):
        # Set up the mock S3 environment
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "lwi-common"
        self.s3.create_bucket(Bucket=self.bucket_name)
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key="lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth_si_1.shp",
            Body=b"test data",
        )

    @mock_s3
    @patch("utils.vector_pipeline.AddData._read_data")
    @patch("utils.vector_pipeline.AddData._AddData__get_regions")
    @patch("main_vector.S3ObjectMonitor")
    def test_monitor_objects(
        self, MockS3ObjectMonitor, mock_read_file, mock_get_regions
    ):
        # Mock the S3ObjectMonitor
        mock_monitor = MockS3ObjectMonitor.return_value
        mock_monitor.monitor_objects.return_value = {
            "added": [
                {
                    "Bucket": "lwi-common",
                    "Key": "lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth_si_1.shp",
                }
            ],
            "removed": [],
        }
        # Mock gpd.read_file to prevent it from trying to read an actual shapefile
        mock_gdf = MagicMock()
        mock_gdf.crs = MagicMock()

        mock_gdf.crs.to_epsg.return_value = 4326
        mock_read_file.return_value = mock_gdf
        mock_gdf.to_crs.return_value = mock_gdf
        mock_get_regions.return_value = [1, 2]

        # Run the main function
        with self.assertLogs(level="INFO") as log:
            main()
        # Check if the logging was called with the expected messages
        self.assertIn(
            "INFO:root:New elements: [{'Bucket': 'lwi-common', 'Key': 'lwi-region0/1_Atlas14_100yr_Upper90_Partial_ PD1Day _V9A1_TD24hr _ARFTP40_maxdepth_si_1.shp'}]",
            log.output,
        )
        self.assertIn("INFO:root:Old elements: []", log.output)


if __name__ == "__main__":
    unittest.main()
