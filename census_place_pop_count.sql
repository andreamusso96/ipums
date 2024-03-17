DROP FUNCTION get_census_place_pop_count(year INTEGER);
CREATE OR REPLACE FUNCTION get_census_place_pop_count(year INTEGER)
RETURNS table(pop_count BIGINT, geom GEOGRAPHY) AS
$$
DECLARE
    geoTableName TEXT := 'geo_' || year;
BEGIN
    RETURN QUERY EXECUTE format($f$
        WITH census_place_counts AS (
            SELECT cpp_placeid, COUNT(*) AS count
            FROM %I
            GROUP BY cpp_placeid
        ),
        unique_geoms AS (
            SELECT DISTINCT ON (cpp_placeid) cpp_placeid, geom
            FROM %I
            ORDER BY cpp_placeid
        )
        SELECT
            a.count AS pop_count,
            b.geom AS geom
        FROM census_place_counts AS a
        JOIN unique_geoms AS b ON a.cpp_placeid = b.cpp_placeid
    $f$, geoTableName, geoTableName);
END;
$$
LANGUAGE plpgsql;