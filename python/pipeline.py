import numpy as np

from python.convolution import get_2d_exponential_kernel, convolve2d
from raster_postgis import load_raster, dump_raster
from utils import get_logger


def prepare_raw_data_tables(con, geo_table_name: str, dem_table_name: str) -> None:
    query = (f"CREATE TABLE {geo_table_name} AS "
             f"SELECT cpp_placeid::BIGINT as census_place_id, geom AS geom, UPPER(histid) AS histid FROM {geo_table_name}_tmp;"
             f"ALTER TABLE {geo_table_name} ADD PRIMARY KEY (histid);"
             f"DROP TABLE {geo_table_name}_tmp;"
             f"ALTER TABLE {dem_table_name} DROP CONSTRAINT {dem_table_name}_pkey;"
             f"ALTER TABLE {dem_table_name} DROP COLUMN ogc_id;"
             f"ALTER TABLE {dem_table_name} ADD PRIMARY KEY (histid);")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def create_census_place_table_and_raster(con, geo_table_name: str, census_place_pop_table_name: str, rasterized_census_place_table_name: str):
    query = (f"CREATE TABLE {census_place_pop_table_name} AS "
             f"SELECT * FROM get_census_place_pop_count('{geo_table_name}');"
             f"CREATE TABLE {rasterized_census_place_table_name} AS "
             f"SELECT * FROM get_rasterized_census_places('{census_place_pop_table_name}');"
             f"SELECT AddRasterConstraints('{rasterized_census_place_table_name}'::name, 'rast'::name);")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def create_clusters(con, rasterized_census_place_table_name: str, convolved_raster_table_name: str, clusters_table_name: str,
                    convolution_kernel_size: int, convolution_kernel_decay_rate: float, pixel_threshold: float, dbscan_eps: float, dbscan_min_points: int):
    raster = load_raster(con=con, raster_table=rasterized_census_place_table_name)
    raster_vals = raster.sel(band=1).values

    kernel = get_2d_exponential_kernel(size=convolution_kernel_size, decay_rate=convolution_kernel_decay_rate)
    convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)

    convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
    dump_raster(con=con, data=convolved_raster, table_name=convolved_raster_table_name)

    query = (f"CREATE TABLE {clusters_table_name} AS "
             f"SELECT cid AS cluster_id, geom FROM convolved_raster_to_cluster('{convolved_raster_table_name}', {pixel_threshold}, {dbscan_eps}, {dbscan_min_points})")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def merge_clusters_demographic_and_geo_data(con, clusters_table_name: str, census_place_pop_table_name: str, geo_table_name: str, demographic_table_name: str, merged_table_name: str):
    query = (f"CREATE TABLE {merged_table_name} AS "
             f"WITH census_place_cluster_crosswalk AS ("
             f"SELECT cluster_id, census_place_id "
             f"FROM {clusters_table_name} LEFT JOIN {census_place_pop_table_name} "
             f"ON ST_Contains({clusters_table_name}.geom, ST_Transform({census_place_pop_table_name}.geom::geometry, 5070)),"
             f"geo_with_cluster AS ("
             f"SELECT histid, cluster_id, census_place_id "
             f"FROM {geo_table_name} LEFT JOIN census_place_cluster_crosswalk "
             f"ON {geo_table_name}.census_place_id = census_place_cluster_crosswalk.census_place_id)"
             f"SELECT {demographic_table_name}.*, census_place_id, cluster_id "
             f"FROM {demographic_table_name} LEFT JOIN geo_with_cluster "
             f"ON {demographic_table_name}.histid = geo_with_cluster.histid")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def run_pipeline(con, from_step: int, year: int, convolution_kernel_size: int, convolution_kernel_decay_rate: float, pixel_threshold: float, cluster_threshold: float, dbscan_eps: float, dbscan_min_points: int):
    logger = get_logger('pipeline')
    logger.info(f"Running pipeline for year {year}")
    logger.info(f"Setting up table names for year {year}")
    geo_table_name = f'geo_{year}'
    census_place_pop_table_name = f"census_place_pop_{year}"
    rasterized_census_place_table_name = f'rasterized_census_place_{year}'
    convolved_raster_table_name = f'convolved_raster_{year}'
    clusters_table_name = f'clusters_{year}'
    demographic_table_name = f'dem_{year}'
    merged_table_name = f'info_{year}'

    logger.info(f"Preparing raw data tables for year {year}")
    if from_step <= 0:
        prepare_raw_data_tables(con=con, geo_table_name=geo_table_name, dem_table_name=demographic_table_name)
    logger.info(f"Creating census place tables for year {year}")
    if from_step <= 1:
        create_census_place_table_and_raster(con=con, geo_table_name=geo_table_name, census_place_pop_table_name=census_place_pop_table_name, rasterized_census_place_table_name=rasterized_census_place_table_name)
    logger.info(f"Creating clusters for year {year}")
    if from_step <= 2:
        create_clusters(con=con, rasterized_census_place_table_name=rasterized_census_place_table_name, convolved_raster_table_name=convolved_raster_table_name, convolution_kernel_size=convolution_kernel_size, convolution_kernel_decay_rate=convolution_kernel_decay_rate,
                        clusters_table_name=clusters_table_name, pixel_threshold=pixel_threshold, dbscan_eps=dbscan_eps, dbscan_min_points=dbscan_min_points)
    logger.info(f"Creating merged table for year {year}")
    if from_step <= 3:
        merge_clusters_demographic_and_geo_data(con=con, clusters_table_name=clusters_table_name, census_place_pop_table_name=census_place_pop_table_name, geo_table_name=geo_table_name, demographic_table_name=demographic_table_name, merged_table_name=merged_table_name)
    logger.info(f"Pipeline for year {year} completed")


if __name__ == '__main__':
    import psycopg2

    conn = psycopg2.connect("dbname='ipums' user='postgres' host='localhost' password='andrea'")
    run_pipeline(con=conn, from_step=0, year=1850, convolution_kernel_size=11, convolution_kernel_decay_rate=0.2, pixel_threshold=100, cluster_threshold=1000, dbscan_eps=100, dbscan_min_points=1)
