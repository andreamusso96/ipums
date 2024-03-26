#!/bin/bash

# Run all the scripts to load the data
chmod +x load_census_data.sh
chmod +x load_geo_data.sh
./load_census_data.sh
./load_geo_data.sh