import numpy as np

from convolution import get_2d_exponential_kernel, convolve2d
from raster_postgis import load_raster, dump_raster
from utils import get_logger


def create_tables(con, year: int):
    census_place_pop_table = f"census_place_pop_{year}"
    query = (f"CREATE TABLE {census_place_pop_table}(pop_count INTEGER, geom GEOGRAPHY);"
             f"INSERT INTO {census_place_pop_table}(pop_count, geom) "
             f"SELECT * FROM get_census_place_pop_count({year});"
             f"CALL rasterize_census_places({year});")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def run_convolution(con, year: int, kernel_size: int, kernel_decay_rate: float):
    raster_table_name = f'rasterized_census_places_{year}'
    convolved_raster_table_name = f'convolved_raster_{year}'

    kernel = get_2d_exponential_kernel(size=kernel_size, decay_rate=kernel_decay_rate)
    raster = load_raster(con=con, raster_table=raster_table_name)
    raster_vals = raster.sel(band=1).values
    convolved_raster_vals = convolve2d(image=raster_vals, kernel=kernel)
    convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
    dump_raster(con=con, data=convolved_raster, table_name=convolved_raster_table_name)


def create_clusters(con, year: int, pixel_threshold: float, cluster_threshold: float, dbscan_eps: float, dbscan_min_points: int):
    clusters_table_name = f'cluster_{year}'
    query = (f"CREATE TABLE {clusters_table_name} AS "
             f"SELECT cid, geom, pop::integer FROM convolved_raster_to_cluster({year}, {pixel_threshold}, {dbscan_eps}, {dbscan_min_points})"
             f"WHERE pop > {cluster_threshold};")

    with con.cursor() as cursor:
        cursor.execute(query)
        con.commit()


def run_pipeline(con, year: int, kernel_size: int, kernel_decay_rate: float, pixel_threshold: float, cluster_threshold: float, dbscan_eps: float, dbscan_min_points: int):
    logger = get_logger('pipeline')
    logger.info(f"Running pipeline for year {year}")
    logger.info(f"Creating tables for year {year}")
    create_tables(con=con, year=year)
    logger.info(f"Running convolution for year {year}")
    run_convolution(con=con, year=year, kernel_size=kernel_size, kernel_decay_rate=kernel_decay_rate)
    logger.info(f"Creating clusters for year {year}")
    create_clusters(con=con, year=year, pixel_threshold=pixel_threshold, cluster_threshold=cluster_threshold, dbscan_eps=dbscan_eps, dbscan_min_points=dbscan_min_points)
    logger.info(f"Pipeline for year {year} completed")


if __name__ == '__main__':
    import psycopg2
    conn = psycopg2.connect("dbname='ipums' user='postgres' host='localhost' password='andrea'")
    run_pipeline(con=conn, year=1850, kernel_size=11, kernel_decay_rate=0.2, pixel_threshold=100, cluster_threshold=1000, dbscan_eps=100, dbscan_min_points=1)