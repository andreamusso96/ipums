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
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE TABLE ${dem_table_name} (year INTEGER, occ1950 INTEGER, ind1950 INTEGER, histid VARCHAR(36), hik VARCHAR(21));"
  psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\COPY ${dem_table_name}(year, occ1950, ind1950, histid, hik) FROM '${file_name_dem_data}' DELIMITER ',' CSV HEADER;"

done

industry_table_data_path="${census_data_path}/industry1950_codes_and_desc.csv"
industry_table_name="industry_1950"
psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE TABLE ${industry_table_name} (code INTEGER, description VARCHAR(70), refined_categories VARCHAR(70), broad_categories VARCHAR(70), agri_non_agri VARCHAR(70), detailed VARCHAR(70), no_agriculture VARCHAR(70), all_group_by VARCHAR (70));"
psql -h "${POSTGRES_HOST}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "\COPY ${industry_table_name}(code, description, refined_categories, broad_categories, agri_non_agri, detailed, no_agriculture, all_group_by) FROM '${industry_table_data_path}' DELIMITER ',' CSV HEADER;"

