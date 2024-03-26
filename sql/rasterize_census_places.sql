DROP FUNCTION IF EXISTS rasterize_census_places(data_table TEXT);
CREATE OR REPLACE FUNCTION rasterize_census_places(data_table TEXT)
RETURNS TABLE(rast raster) AS
$$
BEGIN
    RETURN QUERY EXECUTE format($f$
    WITH usa_raster AS (
        SELECT get_template_usa_raster() AS rast
    ),
    census_place_pop AS (
        WITH census_place_pop_count AS (
            SELECT census_place_id, COUNT(*) AS pop_count
            FROM %I
            GROUP BY census_place_id
        )
        SELECT
            cp_pop_count.pop_count AS pop_count,
            cp.geom AS geom
        FROM census_place_pop_count AS cp_pop_count
        JOIN census_place AS cp
        ON cp_pop_count.census_place_id = cp.id),
   census_places_geomval AS (
       SELECT ARRAY_AGG((ST_Transform(geom::geometry, 5070), pop_count::float)::geomval) AS geomvalset
       FROM census_place_pop
   )
   SELECT ST_SetValues(usa_raster.rast, 1, census_places_geomval.geomvalset, FALSE) AS rast
   FROM census_places_geomval
   CROSS JOIN usa_raster;
        $f$, data_table, data_table);
END;
$$
LANGUAGE plpgsql;