# Go-Consequences Dashboard Workflow

**Project status: Active**

This repo is intended to process the vector data coming from [Go-Consequence](https://github.com/USACE/go-consequences) to store it in a database and define the schema required by the ArcGIS dashboard. It also process raster data (Water Surface Elevation) to automatically read it from an S3 bucket and publish it using ArcGIS GIS Server as a tile map service.

## About the project
The objective of the Louisiana Watershed Initiative (LWI) Go-Consequences Dashboard is to present the results of consequence modeling for: historic events; synthetic events derived from probabilistic combinations of rainfall intensity and duration; and in the coastal areas, annual exceedance probability (AEP) compound flood surfaces, all of which represent different flooding representations across Louisiana. Additionally, the dashboard offers an overlay of flood surfaces corresponding to the selected source and frequency or name, depending on the case, allowing the user not only to see graphical representations of damages, but also the flood depths and extents which drove them. The damage results are summarized based on census tracts, census block groups, and census blocks.  To get more information visit the [help guide](https://experience.arcgis.com/experience/eb850481af654087b2a2f07bd59ba7ed/page/Help/)

## How to run the repository content

Use the `requirements.txt` to install the required dependencies. Note that if you want to run the raster workflow you will need an ArcGIS environment (ArcGIS GIS Server 11.3.1 or ArcGIS Pro 3.3 licensed) in order to work. More different versions can be used by updating the `lwi_template.aprx` and `raster.lyrx` files in `static/arcgis_resources`. 

## Workflow

### Raw data

All the vector data (shapefiles) and raster data (tif files) are stored in a S3 Bucket, organized by LWI Region and in the specific folder for the Go-Consequence data. This data is updated by each region.

### Orchestrator
[Airflow](https://airflow.apache.org/) is set up to run weekly, there are two main tasks, one for the vector data `main_vector.py` and other one for the raster data `main_raster.py`, each task find the new data uploaded and it process only the new or updated data. Also, there is a notification that allow to know if something went wrong. 

### Results

The data processed data would be visualized in the [Go-Consequences Dashboard](https://experience.arcgis.com/experience/eb850481af654087b2a2f07bd59ba7ed)

## Contact 

If you have any comments, questions, or ideas, please feel free to contact us via email at [watershed@thewaterinstitute.org](mailto:watershed@thewaterinstitute.org).

## Note

 This repository is being published for transparency and educational purposes only, and no support will be provided, nor will pull requests be reviewed.