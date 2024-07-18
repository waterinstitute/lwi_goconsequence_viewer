import boto3
import json
from datetime import datetime, timezone


class S3ObjectMonitor:
    """Class that allows identify changes in  a file_type in an specific path in one or more S3 buckets
    params:
        profile_name: AWS profile name : str e.g. "LWI"
        buckets: list of S3 buckets: list e.g. ["bucket1","bucket2"]
        path: path to monitor: str e.g. "path/to/monitor/" (same path in all buckets)
        state_file: file to save the current state: str e.g. "path/state.json"
        last_run_file: file to save the last run: str e.g. "path/last_run.json"
        file_type: file type to monitor: str or tuple e.g. ".shp", ("tiff","tif")"""

    def __init__(
        self, profile_name, buckets, path, state_file, last_run_file, file_type
    ):
        self.profile_name = profile_name
        self.buckets = buckets
        self.path = path
        self.state_file = state_file
        self.last_run_file = last_run_file
        self.__setup_aws_session()
        self.s3 = boto3.client("s3")
        self.file_type = file_type

    def __setup_aws_session(self):
        boto3.setup_default_session(profile_name=self.profile_name)

    # Function to list objects in an S3 bucket
    def list_objects(self, bucket_name, prefix):
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj for obj in response.get("Contents", [])]

    # Function to save the current state to a file
    def save_state(self, state):
        import datetime

        data = self.load_state()
        with open(
            f"{self.state_file[:-5]}_{datetime.datetime.now().strftime('%Y_%m_%d_%H%M%S')}.json",
            "w",
        ) as f:
            json.dump(data, f)
        with open(self.state_file, "w") as f:
            json.dump(state, f)

    # Function to load the previous state from a file
    def load_state(self):
        try:
            with open(self.state_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def load_last_run(self):
        try:
            with open(self.last_run_file, "r") as f:
                return json.load(f)["datetime"]
        except FileNotFoundError:
            return datetime.now(timezone.utc).isoformat()

    def save_last_run(self):
        with open(self.last_run_file, "w") as f:
            json.dump({"datetime": datetime.now(timezone.utc).isoformat()}, f)

    def get_s3_client(self):
        return self.s3

    def monitor_objects(self):
        # Load the previous state
        previous_state = self.load_state()
        # List objects in the S3 buckets
        shp_files = []
        for _b in self.buckets:
            try:
                shp_files.extend(
                    {
                        "Bucket": _b,
                        "Key": x["Key"],
                        "LastModified": x["LastModified"].isoformat(),
                    }
                    for x in self.list_objects(_b, self.path)
                    if x["Key"].endswith(self.file_type)
                )
            except Exception:
                print(f"Error reading {_b}")

        current_state = shp_files
        last_run = self.load_last_run()

        # Compare the current state to the previous state to detect changes
        added_objects = [obj for obj in current_state if obj not in previous_state]
        removed_objects = [obj for obj in previous_state if obj not in current_state]
        updated_objects = [
            obj
            for obj in current_state
            if datetime.fromisoformat(obj["LastModified"])
            > datetime.fromisoformat(last_run)
        ]

        # Update the previous state with the current state
        self.save_state(current_state)

        # Last Run update
        self.save_last_run()
        return {
            "added": added_objects,
            "removed": removed_objects,
            "updated": updated_objects,
        }
