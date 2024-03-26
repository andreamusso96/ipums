import numpy as np
from typing import List

from python.pipeline.convolution import get_2d_exponential_kernel, convolve2d
from python.pipeline.raster_postgis import load_raster, dump_raster
from python.utils import get_logger, execute_sql, get_db_connection

logger = get_logger('pipeline')


# Step 0: preprocess_data

def preprocess_data(census_place_table_name: str, usa_state_geom_table: str, industry_table_name: str, path_sql_functions: str) -> None:
    logger.debug("Preprocessing data")

    logger.debug("Configuring database")
    _configure_db()

    logger.debug("Refactoring census_place table")
    _refactor_census_place_table(census_place_table_name=census_place_table_name)

    logger.debug("Refactoring usa_state_geom table")
    _refactor_usa_state_geom_table(usa_state_geom_table=usa_state_geom_table)

    logger.debug("Refactoring industry table")
    _refactor_industry_table(industry_table_name=industry_table_name)

    logger.debug("Inserting SQL functions")
    _insert_sql_functions(path_sql_functions=path_sql_functions)


def _configure_db():
    query = (f"CREATE EXTENSION IF NOT EXISTS postgis;"
             f"CREATE EXTENSION IF NOT EXISTS postgis_raster;"
             f"ALTER DATABASE ipums SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';")
    execute_sql(query=query)


def _refactor_census_place_table(census_place_table_name: str) -> None:
    query = (f"CREATE TABLE {census_place_table_name}_new (id INTEGER PRIMARY KEY, potential_match VARCHAR(50), geom GEOGRAPHY);"
             f"INSERT INTO {census_place_table_name}_new "
             f"SELECT id, potential_match, geom "
             f"FROM {census_place_table_name};"
             f"DROP TABLE {census_place_table_name};"
             f"ALTER TABLE {census_place_table_name}_new RENAME TO {census_place_table_name};"
             f"CREATE INDEX ON {census_place_table_name} USING GIST (geom);")

    execute_sql(query=query)


def _refactor_usa_state_geom_table(usa_state_geom_table: str) -> None:
    query = (f"CREATE TABLE {usa_state_geom_table}_new (state_code VARCHAR(2) PRIMARY KEY, state_name VARCHAR(25), geom GEOMETRY);"
             f"INSERT INTO {usa_state_geom_table}_new "
             f"SELECT state_code, state_name, geom "
             f"FROM {usa_state_geom_table};"
             f"DROP TABLE {usa_state_geom_table};"
             f"ALTER TABLE {usa_state_geom_table}_new RENAME TO {usa_state_geom_table};")

    execute_sql(query=query)


def _refactor_industry_table(industry_table_name: str) -> None:
    query = f"ALTER TABLE {industry_table_name} ADD PRIMARY KEY (code);"
    execute_sql(query=query)


def _insert_sql_functions(path_sql_functions: str):
    sql_function_files = [f'{path_sql_functions}/{f}' for f in ['template_usa_raster.sql', 'rasterize_census_places.sql', 'convolved_raster_to_cluster.sql']]
    for sql_f in sql_function_files:
        with open(sql_f, 'r') as f:
            query = f.read()
            execute_sql(query=query)
        logger.debug(f"Inserted function from {sql_f}")


# Step 1: create_data_table

def create_data_table(geo_table_name: str, dem_table_name: str, data_table_name: str, census_place_table_name: str, industry_table_name: str) -> None:
    logger.debug(f"Creating data table {data_table_name} from {geo_table_name} and {dem_table_name}")

    logger.debug(f"Transforming histid in {geo_table_name} to uppercase")
    _transform_histid_geo_table_to_uppercase(geo_table_name=geo_table_name)

    logger.debug(f"Creating indices for {geo_table_name} and {dem_table_name}")
    _create_indices_geo_and_dem_tables(geo_table_name=geo_table_name, dem_table_name=dem_table_name)

    logger.debug(f"Removing duplicate histids from {geo_table_name} and {dem_table_name}")
    _remove_duplicate_hist_ids_from_geo_and_dem_tables(geo_table_name=geo_table_name, dem_table_name=dem_table_name)

    logger.debug(f"Merging {geo_table_name} and {dem_table_name} to create {data_table_name}")
    _merge_geo_and_dem_tables_to_create_data_table(geo_table_name=geo_table_name, dem_table_name=dem_table_name, data_table_name=data_table_name)

    logger.debug(f"Adding primary and foreign keys to {data_table_name}")
    _add_primary_and_foreign_keys_to_data_table(data_table_name=data_table_name, census_place_table_name=census_place_table_name, industry_table_name=industry_table_name)

    logger.debug(f"Dropping {geo_table_name} and {dem_table_name}")
    _drop_geo_and_dem_tables(dem_table_name=dem_table_name, geo_table_name=geo_table_name)


def _transform_histid_geo_table_to_uppercase(geo_table_name: str):
    query = (f"CREATE TABLE {geo_table_name}_new (census_place_id INTEGER, histid VARCHAR(36));"
             f"INSERT INTO {geo_table_name}_new "
             f"SELECT cpp_placeid, UPPER(histid) FROM {geo_table_name};"
             f"DROP TABLE {geo_table_name};"
             f"ALTER TABLE {geo_table_name}_new RENAME TO {geo_table_name}")

    execute_sql(query=query)


def _create_indices_geo_and_dem_tables(geo_table_name: str, dem_table_name: str):
    query = (f"CREATE INDEX ON {geo_table_name} (histid);"
             f"CREATE INDEX ON {dem_table_name} (histid);")
    execute_sql(query=query)


def _remove_duplicate_hist_ids_from_geo_and_dem_tables(geo_table_name: str, dem_table_name: str):
    table_names = [geo_table_name, dem_table_name]
    for tab_name in table_names:
        _remove_duplicate_histid_from_table(tab_name=tab_name)


def _remove_duplicate_histid_from_table(tab_name: str):
    query = (f"WITH duplicates AS ("
             f"SELECT histid, ROW_NUMBER() OVER(PARTITION BY histid) AS rownum "
             f"FROM {tab_name})"
             f"DELETE FROM {tab_name} "
             f"USING duplicates "
             f"WHERE {tab_name}.histid = duplicates.histid AND duplicates.rownum > 1;")
    execute_sql(query=query)


def _merge_geo_and_dem_tables_to_create_data_table(geo_table_name: str, dem_table_name: str, data_table_name: str):
    query = (f"CREATE TABLE {data_table_name} (histid VARCHAR(36), hik VARCHAR(21), ind1950 INTEGER, occ1950 INTEGER, census_place_id INTEGER);"
             f"INSERT INTO {data_table_name} "
             f"SELECT {dem_table_name}.histid, NULLIF(hik, '                     '), ind1950, occ1950, CASE WHEN census_place_id > 69491 THEN NULL ELSE census_place_id END AS census_place_id "
             f"FROM {dem_table_name} LEFT JOIN {geo_table_name} "
             f"ON {dem_table_name}.histid = {geo_table_name}.histid;")

    execute_sql(query=query)


def _add_primary_and_foreign_keys_to_data_table(data_table_name: str, census_place_table_name: str, industry_table_name: str):
    query = (f"ALTER TABLE {data_table_name} ADD PRIMARY KEY (histid);"
             f"ALTER TABLE {data_table_name} ADD FOREIGN KEY (census_place_id) REFERENCES {census_place_table_name}(id);"
             f"ALTER TABLE {data_table_name} ADD FOREIGN KEY (ind1950) REFERENCES {industry_table_name}(code);")

    execute_sql(query=query)


def _drop_geo_and_dem_tables(dem_table_name: str, geo_table_name: str):
    query = (f"DROP TABLE {dem_table_name};"
             f"DROP TABLE {geo_table_name};")

    execute_sql(query=query)


# Step 2: create_clusters
def create_clusters(data_table_name: str, rasterized_census_place_table_name: str, cluster_table_name: str, convolved_raster_table_name: str, convolution_kernel_size: int, convolution_kernel_decay_rate: float, pixel_threshold: float, dbscan_eps: float, dbscan_min_points: int) -> None:
    logger.debug(f"Creating clusters table {cluster_table_name} from {data_table_name}")

    logger.debug(f"Rasterizing {data_table_name} to {rasterized_census_place_table_name}")
    _rasterize_census_places(data_table_name=data_table_name, rasterized_census_place_table_name=rasterized_census_place_table_name)

    logger.debug(f"Convolving {rasterized_census_place_table_name} to {convolved_raster_table_name}")
    _convolve_raster(rasterized_census_place_table_name=rasterized_census_place_table_name, convolved_raster_table_name=convolved_raster_table_name, convolution_kernel_size=convolution_kernel_size, convolution_kernel_decay_rate=convolution_kernel_decay_rate)

    logger.debug(f"Creating clusters from {convolved_raster_table_name}")
    _create_clusters_from_raster(convolved_raster_table_name=convolved_raster_table_name, cluster_table_name=cluster_table_name, pixel_threshold=pixel_threshold, dbscan_eps=dbscan_eps, dbscan_min_points=dbscan_min_points)


def _rasterize_census_places(data_table_name: str, rasterized_census_place_table_name: str) -> None:
    query = (f"CREATE TABLE {rasterized_census_place_table_name} AS "
             f"SELECT * FROM rasterize_census_places('{data_table_name}');"
             f"SELECT AddRasterConstraints('{rasterized_census_place_table_name}'::name, 'rast'::name);")

    execute_sql(query=query)


def _convolve_raster(rasterized_census_place_table_name: str, convolved_raster_table_name: str, convolution_kernel_size: int, convolution_kernel_decay_rate: float) -> None:
    con = get_db_connection()
    raster = load_raster(con=con, raster_table=rasterized_census_place_table_name)
    raster_vals = raster.sel(band=1).values

    kernel = get_2d_exponential_kernel(size=convolution_kernel_size, decay_rate=convolution_kernel_decay_rate)
    convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)

    convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
    dump_raster(con=con, data=convolved_raster, table_name=convolved_raster_table_name)
    con.close()


def _create_clusters_from_raster(convolved_raster_table_name: str, cluster_table_name: str, pixel_threshold: float, dbscan_eps: float, dbscan_min_points: int) -> None:
    query = (f"CREATE TABLE {cluster_table_name} AS "
             f"SELECT cid AS cluster_id, geom FROM convolved_raster_to_cluster('{convolved_raster_table_name}', {pixel_threshold}, {dbscan_eps}, {dbscan_min_points});"
             f"CREATE INDEX ON {cluster_table_name} USING GIST (geom);")

    execute_sql(query=query)


# Step 3: create_cluster_industry_table


def create_cluster_data_tables(data_table_name: str, cluster_table_name: str, cluster_industry_table_name: str, industry_table_name: str) -> None:
    logger.debug(f"Creating cluster industry table {cluster_industry_table_name} from {cluster_table_name}")

    logger.debug(f"Creating cluster industry table from {cluster_table_name}")
    _create_cluster_industry_table(data_table_name=data_table_name, cluster_table_name=cluster_table_name, cluster_industry_table_name=cluster_industry_table_name)

    logger.debug(f"Adding population to {cluster_table_name}")
    _create_cluster_cluster_table_with_population(cluster_table_name=cluster_table_name, cluster_industry_table_name=cluster_industry_table_name)

    logger.debug(f"Adding primary and foreign keys to {cluster_industry_table_name}")
    _add_primary_and_foreign_keys_to_cluster_industry_table(cluster_table_name=cluster_table_name, cluster_industry_table_name=cluster_industry_table_name, industry_table_name=industry_table_name)


def _create_cluster_industry_table(data_table_name: str, cluster_table_name: str, cluster_industry_table_name: str) -> None:
    query = (f"CREATE TABLE {cluster_industry_table_name} AS "
             f"WITH cluster_census_place_crosswalk AS ("
             f"SELECT id AS census_place_id, cluster_id "
             f"FROM {cluster_table_name} JOIN census_place "
             f"ON ST_Within(ST_Transform(census_place.geom::geometry, 5070), {cluster_table_name}.geom)), "
             f"industry_counts_census_place AS ("
             f"SELECT census_place_id, ind1950, COUNT(*) AS n_workers "
             f"FROM {data_table_name} "
             f"GROUP BY census_place_id, ind1950) "
             f"SELECT cluster_id, ind1950, SUM(n_workers) AS n_workers "
             f"FROM industry_counts_census_place AS industry "
             f"JOIN cluster_census_place_crosswalk AS crosswalk "
             f"ON industry.census_place_id = crosswalk.census_place_id "
             f"GROUP BY cluster_id, ind1950 "
             f"ORDER BY cluster_id, ind1950; ")

    execute_sql(query=query)


def _create_cluster_cluster_table_with_population(cluster_table_name: str, cluster_industry_table_name: str) -> None:
    query = (f"CREATE TABLE {cluster_table_name}_new AS "
             f"WITH population_counts_cluster AS ("
             f"SELECT cluster_id, SUM(n_workers) AS population "
             f"FROM {cluster_industry_table_name} "
             f"GROUP BY cluster_id) "
             f"SELECT {cluster_table_name}.cluster_id, population, geom "
             f"FROM {cluster_table_name} JOIN population_counts_cluster "
             f"ON {cluster_table_name}.cluster_id = population_counts_cluster.cluster_id; "
             f"DROP TABLE {cluster_table_name};"
             f"ALTER TABLE {cluster_table_name}_new RENAME TO {cluster_table_name};"
             f"CREATE INDEX ON {cluster_table_name} USING GIST (geom);"
             f"ALTER TABLE {cluster_table_name} ADD PRIMARY KEY (cluster_id);")

    execute_sql(query=query)


def _add_primary_and_foreign_keys_to_cluster_industry_table(cluster_table_name: str, cluster_industry_table_name: str, industry_table_name: str) -> None:
    query = (f"ALTER TABLE {cluster_industry_table_name} ADD PRIMARY KEY (cluster_id, ind1950);"
             f"ALTER TABLE {cluster_industry_table_name} ADD FOREIGN KEY (cluster_id) REFERENCES {cluster_table_name}(cluster_id);"
             f"ALTER TABLE {cluster_industry_table_name} ADD FOREIGN KEY (ind1950) REFERENCES {industry_table_name}(code);")

    execute_sql(query=query)


def run_pipeline(steps: List[int], year: int, convolution_kernel_size: int, convolution_kernel_decay_rate: float, pixel_threshold: float, dbscan_eps: float, dbscan_min_points: int):
    logger.info(f"Running pipeline for year {year}")
    logger.info(f"Setting up table names for year {year}")

    # Fix table names
    census_place_table_name = f'census_place'
    usa_state_geom_table = f'usa_state_geom'
    industry_table_name = f'industry_1950'
    path_sql_functions = '/sql'

    # Year specific table names
    geo_table_name = f'geo_{year}'
    demographic_table_name = f'dem_{year}'
    data_table_name = f'census_{year}'

    rasterized_census_place_table_name = f'rasterized_census_place_{year}'
    convolved_raster_table_name = f'convolved_raster_{year}'
    cluster_table_name = f'cluster_{year}'

    cluster_industry_table_name = f'cluster_industry_{year}'

    logger.info(f"Preparing data")
    if 0 in steps:
        preprocess_data(census_place_table_name=census_place_table_name, usa_state_geom_table=usa_state_geom_table, industry_table_name=industry_table_name, path_sql_functions=path_sql_functions)
    logger.info(f"Creating data table for year {year}")
    if 1 in steps:
        create_data_table(geo_table_name=geo_table_name, dem_table_name=demographic_table_name, data_table_name=data_table_name, census_place_table_name=census_place_table_name, industry_table_name=industry_table_name)
    logger.info(f"Creating clusters for year {year}")
    if 2 in steps:
        create_clusters(data_table_name=data_table_name, rasterized_census_place_table_name=rasterized_census_place_table_name, cluster_table_name=cluster_table_name, convolved_raster_table_name=convolved_raster_table_name, convolution_kernel_size=convolution_kernel_size,
                        convolution_kernel_decay_rate=convolution_kernel_decay_rate, pixel_threshold=pixel_threshold, dbscan_eps=dbscan_eps, dbscan_min_points=dbscan_min_points)
    logger.info(f"Creating cluster industry table for year {year}")
    if 3 in steps:
        create_cluster_data_tables(data_table_name=data_table_name, cluster_table_name=cluster_table_name, cluster_industry_table_name=cluster_industry_table_name, industry_table_name=industry_table_name)
    logger.info(f"Pipeline for year {year} completed")


if __name__ == '__main__':
    run_pipeline(steps=[3], year=1930, convolution_kernel_size=11, convolution_kernel_decay_rate=0.2, pixel_threshold=100, dbscan_eps=100, dbscan_min_points=1)
