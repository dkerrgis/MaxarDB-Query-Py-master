# MaxarDB-Query-Py
Package to make multi-threaded queries to WP Maxar Footprint PostGIS Database

## Contents
The main scripts in package are:
* **main.py** - Helper script to instantiate building_query.building_query.QueryDBMulti class, query data based on input file and return/save results
* **building_query/building_query.py** - Module with single thread and multithread database query classes.

## Input Preparation
The script will loop through the polygons of an input shapefile/geopandas.GeoDataFrame in WGS84 and query the maxar PostGIS database for buildings intersecting the input shape. The successful query should return the columns: **gid, wp_iso, area, perimeter, geometry -- wp_iso is iso of country in WorldPop mastergrid where it intersects the buildings (THIS CAN BE NULL IN CASES WHERE MASTERGRID HAS NO COVERAGE)**

The size of individual polygons in the input shapefile cannot exceed 100km<sup>2</sup> at the moment, but the queries can be run asyncronously on multiple threads to speed up the query. One way to tile a shapefile into a grid is to use the Create Fishnet function in ArcGIS Pro (use original shapefile to define bounds) and clip the output Fishnet with the original shapefile.

### Example implementation
    #main.py
    from building_query import QueryDBMulti
    
    shp = <PATH TO INPUT SHAPEFILE> **OR** <geopandas.GeoDataFrame> #Should have geometries with areas < 100km**2
    output_shp = <PATH TO OUTPUT SHAPEFILE> IF YOU WANT TO SAVE THE OUTPUT
    query_table = <schema>.<iso_country_name>
    user = <USER>
    password = <PASSWORD>
    chunksize = <OPTIONAL::DEFAULT = 200> Number of rows from database fed into GeoDataFrame at a time. This can be increased dependent on performance/memory
    num_threads = <OPTIONAL::DEFAULT = 10> Number of threads (asyncronous queries) to run at the same time. This can be increased dependent on performance/memory
    
    query = QueryDBMulti(shp, query_table, user, password, chunksize, num_threads)
    buildings_gdf = query.execute_query_multithread()
    
    ### IF YOU WANT TO SAVE THE OUTPUT ###
    query.save_shp(buildings_gdf, output_shp)
    
## Anaconda Installation

* **Open Anaconda terminal or ArcGIS Pro Python Command Prompt and cd into your working directory**
* **Clone git repo** `git clone https://github.com/wpgp/MaxarDB-Query-Py.git`
* **Create Anaconda environment** `conda env create -n <YOUR_ENVIRONMENT_NAME> -f environment.yml`
* **Activate the new environment** `conda activate <YOUR_ENVIRONMENT_NAME>`
* **Environment should be set up and ready to run scripts** `python <PATH_TO_YOUR_SCRIPT>.py`


