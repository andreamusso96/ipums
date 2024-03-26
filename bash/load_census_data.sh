#!/bin/bash

# Load environment variables
set -a
source ../config.env
set +a

census_data_path="${BASE_DATA_PATH}/${CENSUS_DATA}"

psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\timing"

for year in $(seq 1850 10 1940); do
  file_name_dem_data="${census_data_path}/usa_${year}.csv"
  dem_table_name="dem_${year}"

  # Load the demographic data into the database
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE TABLE ${dem_table_name} (year INTEGER, occ1850 INTEGER, ind1850 INTEGER, histid VARCHAR(36), hik VARCHAR(21));"
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\COPY ${dem_table_name}(year, occ1850, ind1850, histid, hik) FROM '${file_name_dem_data}' DELIMITER ',' CSV HEADER;"

done
