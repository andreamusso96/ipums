#!/bin/bash

# Load environment variables
set -a
source ../config.env
set +a

# Load the US states shapefile into the database
us_shapefile="${BASE_DATA_PATH}/us_shapefiles/States_shapefile.shp"
us_shapefile_table_name='usa_state_geom'
shp2pgsql -s 4326 "${us_shapefile}" "${us_shapefile_table_name}" | psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"
