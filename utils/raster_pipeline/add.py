import logging
import coloredlogs
import os
import arcpy
import string
import yaml
import re
import random
from arcgis.mapping import WebMap
from arcgis.gis import GIS
from .. import (
    configure_mapserver_capabilities,
    activate_cache,
    change_cache_dir,
    share_options,
    edit_scales,
)

log = logging.getLogger(__name__)
coloredlogs.install(level="INFO")


class AddData:
    def __init__(self, path, temp_path, s3, config_file="credentials.yaml"):
        """Define a class to add data and publish a raster
        parameters:
        path: dict - Contains the bucket and key of the file to be processed
        temp_path: str - Path to the temp folder to store temporal resources as the project and the image
        s3: S3 Client using boto3"""
        log.info(" This class will remove the temp folder at the end of the process")
        self.path = path
        self.s3 = s3
        self.temp_path = temp_path
        if not os.path.exists(self.temp_path):
            os.mkdir(self.temp_path)
        self.config_file = config_file
        self._load_config(self.config_file)
        self.s3_path = self._get_s3_path()
        self.min_scale = "9244648.868618"
        self.max_scale = "9027.977411"
        package_directory = os.path.dirname(os.path.abspath(__file__))
        self.template = os.path.join(
            package_directory, "../../static/arcgis_resources/lwi_template.aprx"
        )
        self.symbology = os.path.join(
            package_directory, "../../static/arcgis_resources/raster.lyrx"
        )
        self.raster_path = self._download_raster()
        self.region = self._get_region()
        # Scales defined for each raster
        self.scales = "9244648.868618;4622324.434309;2311162.217155;1155581.108577;577790.554289;288895.277144;"
        self.scales += (
            "144447.638572;72223.819286;36111.909643;18055.954822;9027.977411"
        )

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
            return True

        except FileNotFoundError:
            log.info("Credentials file not found")
            return None

    def _get_s3_path(self) -> str:
        """Return the s3 path of the file to be processed"""
        return f"s3://{self.path['Bucket']}/{self.path['Key']}"

    def _download_raster(self) -> str:
        """Read the data from the s3 bucket and download a tif image
        if it has a projection different than 3857, it projects it to 3857
        return the path to the image"""
        log.info(f" Downloading {self.s3_path}")
        pattern = r"\.(?!(tif|tiff))"
        result = re.sub(pattern, "_", self.path["Key"].split("/")[-1])
        local_path = self.temp_path + result
        temp_full_path = os.path.abspath(self.temp_path)
        try:
            self.s3.download_file(self.path["Bucket"], self.path["Key"], local_path)
        except (OSError, Exception) as e:
            log.error(f" Error downloading {self.s3_path}")
            log.error(e)
            return False
        out_coor_system = arcpy.Describe(os.path.abspath(local_path)).spatialReference
        if out_coor_system.factoryCode != 3857:
            log.info(f" Projecting {self.s3_path} to 3857")
            sr = arcpy.SpatialReference(3857)
            new_path = re.sub(r"\.(tif|tiff)$", r"_proj.\1", local_path)
            arcpy.ProjectRaster_management(local_path, os.path.abspath(new_path), sr)
            os.remove(local_path)
            files = [f for f in os.listdir(temp_full_path)]
            for f in files:
                source_file = os.path.join(temp_full_path, f)
                destination_file = os.path.join(temp_full_path, f.replace("_proj", ""))
                os.rename(source_file, destination_file)
            log.info(f" {self.s3_path} projected to 3857")
        return os.path.abspath(local_path)

    def create_project(self) -> str:
        """Create a project in the temp folder, adding image and symbology to it"""
        project_temp = arcpy.mp.ArcGISProject(self.template)

        m_temp = project_temp.listMaps()[0]
        m_temp.addDataFromPath(self.raster_path)
        lyrs_temp = [m_temp.listLayers()[0]]
        lyrs_temp[0].name = lyrs_temp[0].name.replace(".tiff", "").replace(".tif", "")
        self.service_name = lyrs_temp[0].name
        new_project_path = os.path.join(
            os.path.abspath(self.temp_path), f"{self.service_name}.aprx"
        )
        log.info(self.service_name)
        arcpy.ApplySymbologyFromLayer_management(lyrs_temp[0], self.symbology)
        project_temp.saveACopy(new_project_path)
        ## Adding elements from the new project
        self.project = arcpy.mp.ArcGISProject(new_project_path)
        self.m = self.project.listMaps()[0]
        self.lyrs = [self.m.listLayers()[0]]

        return new_project_path

    def create_draft(self, cache_dir="/cloudStores/lwi_goconsequence_cache"):
        """Create a draft for the raster service"""
        self.sddraftPath = os.path.abspath(
            os.path.join(self.temp_path, self.service_name + ".sddraft")
        )
        server_type = "FEDERATED_SERVER"
        sharing_draft = self.m.getWebLayerSharingDraft(
            server_type, "MAP_IMAGE", self.service_name, self.lyrs
        )
        sharing_draft.federatedServerUrl = self.serverUrl
        sharing_draft.summary = (
            f"{self.region} GoConsequence result: {self.service_name}"
        )
        sharing_draft.tags = "LWI, Raster, GoConsequence"
        sharing_draft.description = (
            f"{self.region} GoConsequence result: {self.service_name}"
        )
        sharing_draft.credits = "LWI, TWI"
        sharing_draft.useLimitations = "Copytright"
        sharing_draft.portalFolder = self.serverFolder
        sharing_draft.serverFolder = self.serverFolder
        sharing_draft.copyDataToServer = False
        sharing_draft.exportToSDDraft(self.sddraftPath)
        configure_mapserver_capabilities(self.sddraftPath, "Map")
        activate_cache(self.sddraftPath)
        change_cache_dir(cache_dir, self.sddraftPath)
        # Change following to "true" to share
        SharetoOrganization = "false"
        SharetoEveryone = "true"
        SharetoGroup = "false"
        # if there are more than one Put the ID seaparated by commas
        GroupID = ""
        share_options(
            SharetoOrganization,
            SharetoEveryone,
            SharetoGroup,
            self.sddraftPath,
            GroupID,
        )
        edit_scales(self.sddraftPath, self.min_scale, self.max_scale)

    def _get_region(self) -> str:
        """Get the region from the s3 path"""
        try:
            region = int(self.path["Bucket"][-1])
        except ValueError:
            region = 0

        return f"Region {region}"

    def publish_raster(self) -> None:
        """Publish the raster service usig the dratf created"""
        self.sdPath = os.path.abspath(
            os.path.join(self.temp_path, self.service_name + ".sd")
        )
        input_service = (
            self.serverUrl
            + "/rest/services/"
            + self.serverFolder
            + "/"
            + self.service_name
            + "/MapServer"
        )

        try:
            arcpy.server.StageService(
                self.sddraftPath, self.sdPath, staging_version=209
            )
            arcpy.server.UploadServiceDefinition(
                self.sdPath, self.serverUrl, in_public="PUBLIC"
            )
            warnings = arcpy.GetMessages(1)
            log.info(warnings)

        except Exception as stage_exception:
            log.error(
                " Sddraft not staged. Analyzer errors encountered - {}".format(
                    str(stage_exception)
                )
            )
        # Generating cache only for scales defined
        arcpy.server.ManageMapServerCacheTiles(
            input_service, self.scales, "RECREATE_ALL_TILES"
        )

    def add_to_webmap(self) -> bool:
        """Add the raster to the webmap"""
        try:
            gis = GIS(
                self.portalUrl, self.portalUser, self.portalPass, verify_cert=True
            )

            for item_s in gis.content.search(self.webmapName, item_type="Web Map"):
                log.info(f"Working on {item_s.title} webmap")
                item = gis.content.get(item_s.id)
                wm = WebMap(item)
                layer_id = gis.content.search(self.service_name)[0].id
                layer_item = gis.content.get(layer_id)
                new_map_layer = {
                    "id": self.create_layer_id(random.randint(100, 99999)),
                    "url": layer_item.url,
                    "title": layer_item.layers[0].properties.name,
                    "visibility": False,
                    "itemId": layer_item.id,
                    "layerType": "ArcGISTiledMapServiceLayer",
                }
                log.info(self.region)
                log.info("-------Region------")
                region_idx = self.get_region_index(self.region, wm.layers)
                wm.layers[region_idx]["layers"].append(new_map_layer)
                wm.update()
            return True
        except Exception as e:
            log.error(f" Error adding {self.s3_path} to the webmap")
            log.error(e)
            return False

    def create_layer_id(self, layerIndex: int) -> str:
        """Create a layer id for the webmap"""
        return (
            "".join(random.choices(string.ascii_lowercase + string.digits, k=11))
            + "-layer-"
            + str(layerIndex)
        )

    def get_region_index(self, region_name: str, layers: list) -> int:
        """Get the index of the region in the webmap"""
        for i, dictionary in enumerate(layers):
            if dictionary.get("title") == region_name:
                return i

    def clean_local(self) -> None:
        """Clean the local resources"""
        import shutil

        shutil.rmtree(self.temp_path)

    def execute(self) -> bool:
        """Execute the pipeline"""
        self.create_project()
        self.create_draft()
        try:
            log.info(f" Creating Project for {self.s3_path}")

            log.info(f" Creating draft for {self.s3_path}")
            self.create_draft()
            log.info(f" Publishing {self.s3_path}")
            self.publish_raster()
            log.info(f" Adding {self.s3_path} to webmap")
            self.add_to_webmap()
            log.info(f" Cleaning local resources for {self.s3_path}")
            self.clean_local()
            log.info(f" Finished processing {self.s3_path}")
            return True
        except Exception as e:
            log.error(f" Error processing {self.s3_path}")
            log.error(e)
            return False
