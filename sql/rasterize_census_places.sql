DROP FUNCTION IF EXISTS get_rasterized_census_places(census_place_pop_table_name TEXT);
CREATE OR REPLACE FUNCTION get_rasterized_census_places(census_place_pop_table_name TEXT)
RETURNS TABLE(rast raster) AS
$$
BEGIN
    RETURN QUERY EXECUTE format($f$
    WITH us_raster AS (
        SELECT get_template_usa_raster() AS rast
    ),
   census_places_geomval AS (
       SELECT ARRAY_AGG((ST_Transform(geom::geometry, 5070), pop_count::float)::geomval) AS geomvalset
       FROM %I
   )
   SELECT ST_SetValues(us_raster.rast, 1, census_places_geomval.geomvalset, FALSE) AS rast
   FROM census_places_geomval
   CROSS JOIN us_raster;
    $f$, census_place_pop_table_name);
END;
$$
LANGUAGE plpgsql;