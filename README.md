# Go-Consequences Dashboard Workflow

**Project status: Active**

This repository is intended to be used to process vector data coming from [Go-Consequences](https://github.com/USACE/go-consequences) so that data can be store in a database with the defined schema required by the ArcGIS dashboard. This repository also processes raster data (water surface elevation) so it can be automatically read from an S3 bucket and published using ArcGIS GIS Server as a tile map service.

## About the project
The objective of the Louisiana Watershed Initiative (LWI) Go-Consequences Dashboard is to present the results of consequence modeling for historic events, synthetic events derived from probabilistic combinations of rainfall intensity and duration, and in coastal areas—annual exceedance probability (AEP) compound flood surfaces, all of which provide different flooding representations across Louisiana. Additionally, the dashboard offers an overlay of flood surfaces corresponding to the selected source and frequency or name, depending on the case, allowing the user not only to see graphical representations of damages, but also the flood depths and extents which drove them. The damage results are summarized based on census tracts, census block groups, and census blocks. For more information visit the [help guide](https://experience.arcgis.com/experience/eb850481af654087b2a2f07bd59ba7ed/page/Help/)

## How to run the repository content

Use the `requirements.txt` to install the required dependencies. Note that if you want to run the raster workflow you will need an ArcGIS environment (ArcGIS GIS Server 11.3.1 or ArcGIS Pro 3.3 licensed). Additional ArcGIS versions can be used by updating the `lwi_template.aprx` and `raster.lyrx` files in `static/arcgis_resources`. 

## Workflow

### Raw data

All the vector data (shapefiles) and raster data (tif files) are stored in a S3 Bucket. These data are organized by LWI region and are located in the specific folder for Go-Consequences data. These data are updated by each region.

### Orchestrator
[Airflow](https://airflow.apache.org/) is set up to run weekly. There are two main tasks, one for the vector data `main_vector.py` and one for the raster data `main_raster.py`. Each task identifies newly uploaded data and it processes only new or updated data. There is a notification provided if there is an error in these tasks. 

### Results

The processed data is visualized in the [Go-Consequences Dashboard](https://experience.arcgis.com/experience/eb850481af654087b2a2f07bd59ba7ed)

## Contact 

If you have any comments, questions, or ideas, please feel free to contact us via email at [watershed@thewaterinstitute.org](mailto:watershed@thewaterinstitute.org).

## Note

 This repository is published for transparency and educational purposes only, and no support will be provided, nor will pull requests be reviewed.
