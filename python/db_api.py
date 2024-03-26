from typing import List, Union

import numpy as np
import pandas as pd

from python.utils import sql_to_pandas, get_db_connection, next_census_year, get_logger

cluster_table_name = 'cluster_'
cluster_industry_table_name = f'cluster_industry_'
logger = get_logger('db_api')


def get_cluster_ids(year: int, pop_low: int = 0, pop_high: int = None) -> List[int]:
    pop_high_ = 10**10 if pop_high is None else pop_high
    query = (f"SELECT cluster_id FROM {cluster_table_name}{year} "
             f"WHERE population >= {pop_low} AND population <= {pop_high_}")

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(query)
        cluster_ids = cursor.fetchall()
    con.close()

    cluster_ids = [cid[0] for cid in cluster_ids]
    return cluster_ids


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

    cluster_population = pd.DataFrame(cluster_population, columns=['cluster_id', 'population']).set_index('cluster_id')
    return cluster_population


def get_cluster_growth_rate_distribution(year: int, cluster_ids: List[int] = None) -> np.ndarray:
    cluster_ids = _process_cluster_ids(year=year, cluster_ids=cluster_ids)
    next_year = next_census_year(year=year)
    con = get_db_connection()

    with con.cursor() as cursor:
        query = (f"WITH c1 AS (SELECT * FROM {cluster_table_name}{year} WHERE cluster_id = ANY(%s)) "
                 f"SELECT c1.cluster_id AS id1, c1.population AS pop1, c2.cluster_id AS id2, c2.population AS pop2, COUNT(c1.cluster_id) OVER(PARTITION BY c1.cluster_id) AS app1, COUNT(c2.cluster_id) OVER(PARTITION BY c2.cluster_id) AS app2 "
                 f"FROM c1 JOIN {cluster_table_name}{next_year} c2 "
                 f"ON ST_Intersects(c1.geom, c2.geom)")
        cursor.execute(query, (cluster_ids,))
        cluster_growth = cursor.fetchall()
    con.close()

    cluster_growth = pd.DataFrame(cluster_growth, columns=['id_1', 'pop_1', 'id_2', 'pop_2', 'app_1', 'app_2']).astype({'id_1': int, 'pop_1': float, 'id_2': int, 'pop_2': float, 'app_1': int, 'app_2': int})
    cluster_growth_rate_distribution = []

    # Growth of cluster that appear more than once in both years
    _drop_smaller_clusters_in_many_to_many_matching(cluster_growth=cluster_growth)

    # One-to-one: Growth of cluster that appear once in both years
    cluster_growth_one_to_one = cluster_growth.loc[(cluster_growth['app_1'] == 1) & (cluster_growth['app_2'] == 1)]
    cluster_growth_rate_distribution += (cluster_growth_one_to_one['pop_2'] / cluster_growth_one_to_one['pop_1'] - 1).values.tolist()
    cluster_growth = cluster_growth.drop(index=cluster_growth_one_to_one.index, axis=0)

    # Merges: Growth of cluster that appear more than once in final year
    cluster_growth_many_to_one = cluster_growth.loc[(cluster_growth['app_1'] == 1) & (cluster_growth['app_2'] > 1)]
    cluster_growth_rate_distribution += _aggregate_cluster_growth(cluster_growth=cluster_growth_many_to_one, group_by_col='id_2', fagg_col1='sum', fagg_col2='first')
    cluster_growth = cluster_growth.drop(index=cluster_growth_many_to_one.index, axis=0)

    # Splits: Growth of cluster that appear more than once in initial year
    cluster_growth_one_to_many = cluster_growth.loc[(cluster_growth['app_1'] > 1) & (cluster_growth['app_2'] == 1)]
    cluster_growth_rate_distribution += _aggregate_cluster_growth(cluster_growth=cluster_growth_one_to_many, group_by_col='id_1', fagg_col1='first', fagg_col2='sum')

    cluster_growth_rate_distribution = np.array(cluster_growth_rate_distribution)
    return cluster_growth_rate_distribution


def _drop_smaller_clusters_in_many_to_many_matching(cluster_growth: pd.DataFrame):
    cluster_growth_many_to_many = cluster_growth.loc[(cluster_growth['app_1'] > 1) & (cluster_growth['app_2'] > 1)]
    if not cluster_growth_many_to_many.empty:
        cluster_1_many_to_many_matching = cluster_growth.loc[(cluster_growth['id_1'].isin(cluster_growth_many_to_many['id_1']))]
        for id1 in cluster_1_many_to_many_matching['id_1'].unique():
            id2_clusters = cluster_1_many_to_many_matching.loc[cluster_1_many_to_many_matching['id_1'] == id1, ['id_2', 'pop_2']]
            indices_to_drop = id2_clusters.drop(index=id2_clusters['pop_2'].idxmax(), axis=0).index
            cluster_growth = cluster_growth.drop(index=indices_to_drop, axis=0)


def _aggregate_cluster_growth(cluster_growth, group_by_col, fagg_col1, fagg_col2):
    cluster_growth_agg = cluster_growth.groupby(group_by_col).agg({'pop_1': fagg_col1, 'pop_2': fagg_col2})
    cluster_growth_rate_distribution = (cluster_growth_agg['pop_2'] / cluster_growth_agg['pop_1'] - 1).values.tolist()
    return cluster_growth_rate_distribution


def get_cluster_industry_n_workers(year: int, cluster_ids: List[int] = None, industry_codes: List[int] = None):
    cluster_ids_ = _process_cluster_ids(year=year, cluster_ids=cluster_ids)
    industry_codes_ = _process_industry_codes(industry_codes=industry_codes)

    con = get_db_connection()
    with con.cursor() as cursor:
        cursor.execute(f"SELECT cluster_id, ind1950, n_workers FROM {cluster_industry_table_name}{year} "
                       f"WHERE cluster_id = ANY(%s) AND ind1950 = ANY(%s)", (cluster_ids_, industry_codes_))
        cluster_population = cursor.fetchall()
    con.close()

    cluster_population = pd.DataFrame(cluster_population, columns=['cluster_id', 'industry_code', 'n_workers']).set_index('cluster_id')
    return cluster_population


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
    print(get_cluster_ids(year=1860, pop_low=1000, pop_high=None))
    print(get_industry_codes())
    print(get_cluster_population(year=1860, cluster_ids=[1, 2, 3]))
    print(get_cluster_growth_rate_distribution(year=1860, cluster_ids=None))
    print(get_cluster_industry_n_workers(year=1860, cluster_ids=[1, 2, 3], industry_codes=[0, 105]))