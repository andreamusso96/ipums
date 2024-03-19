DROP FUNCTION IF EXISTS get_census_place_pop_count(geo_table_name TEXT);
CREATE OR REPLACE FUNCTION get_census_place_pop_count(geo_table_name TEXT)
RETURNS table(census_place_id BIGINT, pop_count BIGINT, geom GEOGRAPHY) AS
$$
BEGIN
    RETURN QUERY EXECUTE format($f$
        WITH census_place_counts AS (
            SELECT census_place_id, COUNT(*) AS count
            FROM %I
            GROUP BY census_place_id
        ),
        unique_geoms AS (
            SELECT DISTINCT ON (census_place_id) census_place_id, geom
            FROM %I
            ORDER BY census_place_id
        )
        SELECT
            a.census_place_id AS census_place_id,
            a.count AS pop_count,
            b.geom AS geom
        FROM census_place_counts AS a
        JOIN unique_geoms AS b
        ON a.census_place_id = b.census_place_id
    $f$, geo_table_name, geo_table_name);
END;
$$
LANGUAGE plpgsql;