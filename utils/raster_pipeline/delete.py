import yaml
import logging
import arcpy
from arcgis.mapping import WebMap
from arcgis.gis import GIS

log = logging.getLogger(__name__)


class DeleteData:
    def __init__(self, path, s3, config_file="credentials.yaml"):
        """Define a class to delete cache, remove from a webmap a raster punlished
        parameters:
        path: dict - Contains the bucket and key of the file to be processed
        s3: S3 Client using boto3"""
        self.path = path
        self.s3 = s3
        self.config_file = config_file

        self.s3_path = self._get_s3_path()
        self.service_name = (
            self.path["Key"]
            .split("/")[-1]
            .replace(".tiff", "")
            .replace(".tif", "")
            .replace(".", "_")
        )
        self._load_config(self.config_file)
        self.region_int = self._get_region_int()

    def _load_config(self, config_file: str) -> bool:
        """Load the portal credentials from the yaml file"""
        try:
            with open(config_file, "r") as f:
                config_data = yaml.safe_load(f)
            self.portalUrl = config_data["portal"]["portalUrl"]
            self.serverUrl = config_data["portal"]["serverUrl"]
            self.serverFolder = config_data["portal"]["serverFolder"]
            self.portalUser = config_data["portal"]["username"]
            self.portalPass = config_data["portal"]["password"]
            self.webmapName = config_data["portal"]["webmap"]
            self.portalConnection = arcpy.SignInToPortal(
                self.portalUrl, self.portalUser, self.portalPass
            )
            self.input_service = (
                self.serverUrl
                + "/rest/services/"
                + self.serverFolder
                + "/"
                + self.service_name
                + "/MapServer"
            )
            return True

        except FileNotFoundError:
            log.info("Credentials file not found")
            return False

    def _get_s3_path(self) -> str:
        """Return the s3 path of the file to be processed"""
        return f"s3://{self.path['Bucket']}/{self.path['Key']}"

    def remove_from_webmap(self, region_idx: int) -> bool:
        try:
            """Remove the tile layer from the webmap"""
            self.gis = GIS(
                self.portalUrl, self.portalUser, self.portalPass, verify_cert=True
            )

            for item_s in self.gis.content.search(self.webmapName, item_type="Web Map"):
                log.info(f"Webmap ID {item_s.id}")
                item = self.gis.content.get(item_s.id)
                wm = WebMap(item)
                if region_idx == 0:
                    region_idx = -1
                layers = list(wm.layers[region_idx].layers)
                idx = None
                for i, dictionary in enumerate(layers):
                    if dictionary.get("title") == self.service_name:
                        idx = i
                log.info(f"layer to remove has the index {idx}")
                if idx is not None:
                    layers.remove(layers[idx])
                    wm.layers[region_idx].layers = [] if len(layers) == 0 else layers
                    wm.update()
                    log.info(f"layer {self.service_name} removed")
            return True
        except Exception as e:
            log.error(f"Error removing {self.s3_path} to the webmap")
            log.error(e)
            return False

    def delete_layer(self) -> bool:
        """Delete the layer from the server/portal"""
        try:
            layer_id = self.gis.content.search(self.service_name)[0].id
            layer_item = self.gis.content.get(layer_id)
            layer_item.delete()
            return True
        except Exception as e:
            log.error(f"Error deleting {self.s3_path} from the webmap")
            log.error(e)
            return False

    def delete_cache(self) -> bool:
        """Delete the cache for the service"""
        try:
            arcpy.server.DeleteMapServerCache(self.input_service)
        except Exception as e:
            log.error(f"Error deleting the cache for {self.s3_path}")
            log.error(e)
            return False

    def _get_region_int(self) -> int:
        """Get the region from the s3 path"""
        try:
            region = int(self.path["Bucket"][-1])
        except ValueError:
            region = 0

        return region

    def execute(self) -> bool:
        """Execute the pipeline"""
        try:
            log.info(f"Removing {self.s3_path} from the webmap")
            self.remove_from_webmap(self.region_int)
            log.info(f"Deleting cache for {self.s3_path}")
            self.delete_cache()
            log.info(f"Deleting service {self.input_service}")
            self.delete_layer()
            log.info(f"{self.s3_path} deleted")
            return True
        except Exception as e:
            log.error(f"Error deleting {self.s3_path}")
            log.error(e)
            return False
