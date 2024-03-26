us_shapefile="${BASE_DATA_PATH}/us_shapefiles/States_shapefile.shp"
us_shapefile_table_name='usa_geom'
shp2pgsql -s 4326 "${us_shapefile}" "${us_shapefile_table_name}" | psql -h "${DB_HOST}" -d "${DB_NAME}" -U "${DB_USER}" -w