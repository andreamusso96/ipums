#!/bin/bash

# Load environment variables
set -a
source ../config.env
set +a

geo_data_path="${BASE_DATA_PATH}/${GEO_DATA}"

psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\timing"

for year in $(seq 1850 10 1940); do
  filename_geo_data="${geo_data_path}/histid_place_crosswalk_${year}.csv"
  geo_table_name="geo_${year}"

  # Load the geographic data into the database
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE TABLE ${geo_table_name} (potential_match VARCHAR(50), match_type VARCHAR(50), lat FLOAT, lon FLOAT, state_fips_geomatch VARCHAR(2), county_fips_geomatch VARCHAR(5), cluster_k5 INTEGER, cpp_placeid INTEGER, histid VARCHAR(36));"
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\COPY ${geo_table_name}(potential_match, match_type, lat, lon, state_fips_geomatch, county_fips_geomatch, cluster_k5, cpp_placeid, histid) FROM '${filename_geo_data}' DELIMITER ',' CSV HEADER;"
done

file_name_census_place_data="${geo_data_path}/place_component_crosswalk.csv"
census_place_table_name="census_place"
ogr2ogr -f "PostgreSQL" PG:"dbname=${POSTGRES_DB} host=${POSTGRES_HOST} port=${POSTGRES_PORT} user=${POSTGRES_USER} password=${POSTGRES_PASSWORD}" "${file_name_census_place_data}" \
   -nln "${census_place_table_name}" -nlt POINT -a_srs EPSG:4326 -oo X_POSSIBLE_NAMES=lon -oo Y_POSSIBLE_NAMES=lat -lco GEOMETRY_NAME=geom \
   -lco GEOM_TYPE=geography -lco COLUMN_TYPES='lat=FLOAT,lon=FLOAT,potential_match=VARCHAR(50),id=INTEGER' --config PG_USE_COPY YES

