# Go-Consequences Dashboard Workflow

This repo is intended to process the vector data coming from Go-Consequence to store it in a database and define the schema required by the ArcGIS dashboard. It also process raster data (Water Surface Elevation) to automatically read it from an S3 bucket and publish it using ArcGIS GIS Server as a tile map service.

## How to run the repository content

Use the `requirements.txt` to install the required dependencies. Note that if you want to run the raster workflow you will need an ArcGIS environment (ArcGIS GIS Server 10.9.1 or ArcGIS Pro 2.9 licensed) in order to work. More recent versions can be used by updating the `lwi_template.aprx` and `raster.lyrx` files. 

## Workflow

### Raw data

All the vector data (shapefiles) and raster data (tif files) are stored in a S3 Bucket, organized by LWI Region and in the specific folder for the Go-Consequence data. This data is updated by each region.

### Orchestrator
[Airflow](https://airflow.apache.org/) is set up to run weekly, there are two main tasks, one for the vector data `main_vector.py` and other one for the raster data `main_raster.py`, each task find the new data uploaded and it process only the new or updated data. Also, there is a notification that allow to know if something went wrong. 

### Results

The data processed data would be visualized in the [Go-Consequences Dashboard](https://experience.arcgis.com/experience/eb850481af654087b2a2f07bd59ba7ed)

## Contact 

If you have any comments, questions, or ideas, please feel free to contact us via email at [watershed@thewaterinstitute.org](mailto:watershed@thewaterinstitute.org).
