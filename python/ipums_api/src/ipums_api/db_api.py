from typing import List, Union, Dict
from enum import Enum

import numpy as np
import pandas as pd
import networkx as nx
import geopandas as gpd

from .utils import get_db_connection, next_census_year, get_logger

cluster_table_name = 'cluster_'
cluster_industry_table_name = f'cluster_industry_'
logger = get_logger('db_api')


class IndustryClassification(Enum):
    BASE_CODES = 'code'
    NO_AGRICULTURE = 'no_agriculture'
    DETAILED = 'detailed'
    AGRICULTURE_AND_NON_AGRICULTURE = 'agri_non_agri'
    REFINED_CATEGORIES = 'refined_categories'
    BROAD_CATEGORIES = 'broad_categories'
    ALL = 'all_group_by'


def get_cluster_ids(year: int, pop_low: int = 0, pop_high: int = None) -> List[int]:
    pop_high_ = 10 ** 10 if pop_high is None else pop_high
    query = (f"SELECT cluster_id FROM {cluster_table_name}{year} "
             f"WHERE population >= {pop_low} AND population <= {pop_high_}")

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(query)
        cluster_ids = cursor.fetchall()
    con.close()

    cluster_ids = [cid[0] for cid in cluster_ids]
    return cluster_ids


def get_cluster_geometry(year: int, cluster_ids: List[int] = None) -> pd.DataFrame:
    cluster_ids_ = _process_cluster_ids(year=year, cluster_ids=cluster_ids)
    con = get_db_connection()
    query = f"SELECT cluster_id, geom FROM {cluster_table_name}{year} WHERE cluster_id = ANY(%s)"
    cluster_geo = gpd.GeoDataFrame.from_postgis(query, con, params=(cluster_ids_,), geom_col='geom')
    cluster_geo = cluster_geo.set_index('cluster_id')
    con.close()
    return cluster_geo


def get_census_places() -> pd.DataFrame:
    con = get_db_connection()
    query = "SELECT * FROM census_place"
    census_places = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geom')
    con.close()
    return census_places


def get_census_place_raster(year: int, convolved: bool = True) -> pd.DataFrame:
    raster_table_name = f'convolved_raster_{year}' if convolved else f'rasterized_census_places_{year}'
    con = get_db_connection()
    query = (f"WITH pixels AS ("
             f"SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).* FROM {raster_table_name})"
             f"SELECT val AS population, geom AS geom "
             f"FROM pixels;")
    census_place_raster = gpd.GeoDataFrame.from_postgis(query, con, geom_col='geom')
    con.close()
    return census_place_raster


def get_industry_codes() -> List[int]:
    query = f"SELECT code FROM industry_1950"

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(query)
        industry_codes = cursor.fetchall()
    con.close()

    industry_codes = [cid[0] for cid in industry_codes]
    return industry_codes


def get_cluster_population(year: int, cluster_ids: List[int] = None) -> pd.DataFrame:
    cluster_ids_ = _process_cluster_ids(year=year, cluster_ids=cluster_ids)

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(f"SELECT cluster_id, population FROM {cluster_table_name}{year} "
                       f"WHERE cluster_id = ANY(%s)", (cluster_ids_,))
        cluster_population = cursor.fetchall()
    con.close()

    cluster_population = pd.DataFrame(cluster_population, columns=['cluster_id', 'population']).astype({'cluster_id': int, 'population': float}).set_index('cluster_id')
    return cluster_population


def get_cluster_multiyear_matching(year_start: int, year_end: int) -> List[Dict]:
    matching_graph = nx.Graph()
    current_year = year_start
    while current_year < year_end:
        next_year = next_census_year(year=current_year)
        matching_year = _get_cluster_intersection_matching(year=current_year)
        edges = [(f"{current_year}_{row['id_1']}", f"{next_year}_{row['id_2']}") for i, row in matching_year.iterrows()]
        matching_graph.add_edges_from(edges)
        current_year = next_year

    connected_components = list(nx.connected_components(matching_graph))

    matching_json = []
    for i, component in enumerate(connected_components):
        component_dict = {'component_id': i}
        for node in component:
            year, cluster_id = (int(x) for x in node.split('_'))
            if year not in component_dict:
                component_dict[year] = [cluster_id]
            else:
                component_dict[year].append(cluster_id)

        years = sorted(k for k in component_dict if k != 'component_id')
        component_dict['years'] = years
        matching_json.append(component_dict)

    return matching_json


def _get_cluster_intersection_matching(year: int) -> pd.DataFrame:
    next_year = next_census_year(year=year)
    con = get_db_connection()

    with con.cursor() as cursor:
        query = (f"SELECT c1.cluster_id AS id1, c2.cluster_id AS id2 "
                 f"FROM {cluster_table_name}{year} c1 JOIN {cluster_table_name}{next_year} c2 "
                 f"ON ST_Intersects(c1.geom, c2.geom)")
        cursor.execute(query)
        matching = cursor.fetchall()
    con.close()

    matching = pd.DataFrame(matching, columns=['id_1', 'id_2']).astype({'id_1': int, 'id_2': int})
    return matching


def get_cluster_industry_n_workers(year: int, cluster_ids: List[int] = None, industry_classification: IndustryClassification = IndustryClassification.BASE_CODES) -> pd.DataFrame:
    cluster_ids_ = _process_cluster_ids(year=year, cluster_ids=cluster_ids)

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(f"WITH cluster_industry_n_workers AS ( "
                       f"SELECT cluster_id, ind1950, n_workers FROM {cluster_industry_table_name}{year} "
                       f"WHERE cluster_id = ANY(%s))"
                       f"SELECT cluster_id, {industry_classification.value} AS industry_code, SUM(n_workers) AS n_workers "
                       f"FROM cluster_industry_n_workers JOIN industry_1950 "
                       f"ON cluster_industry_n_workers.ind1950 = industry_1950.code "
                       f"GROUP BY cluster_id, industry_code", (cluster_ids_,))
        n_workers_by_cluster_and_industry = cursor.fetchall()
    con.close()

    n_workers_by_cluster_and_industry = pd.DataFrame(n_workers_by_cluster_and_industry, columns=['cluster_id', 'industry_code', 'n_workers']).astype({'cluster_id': int, 'industry_code': str, 'n_workers': float})
    return n_workers_by_cluster_and_industry


def _process_cluster_ids(year: int, cluster_ids: Union[List[int], np.ndarray, None]) -> List[int]:
    if cluster_ids is None:
        return get_cluster_ids(year=year)
    else:
        cluster_ids = [int(cid) for cid in cluster_ids]
        return cluster_ids


def _process_industry_codes(industry_codes: Union[List[int], np.ndarray, None]) -> List[int]:
    if industry_codes is None:
        return get_industry_codes()
    else:
        industry_codes = [int(cid) for cid in industry_codes]
        return industry_codes


if __name__ == '__main__':
    print(get_cluster_industry_n_workers(year=1860, cluster_ids=[1, 2, 3], industry_classification=IndustryClassification.DETAILED))
