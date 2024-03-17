DROP PROCEDURE IF EXISTS rasterize_census_places(input_year INT);
CREATE OR REPLACE PROCEDURE rasterize_census_places(input_year INT)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variable to hold the dynamic table name based on the input year
    census_place_pop TEXT := 'census_place_pop_' || input_year;
    rasterized_census_places TEXT := 'rasterized_census_place_' || input_year;
BEGIN
    EXECUTE format($f$
        CREATE TABLE %I AS
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
        SELECT AddRasterConstraints('%I'::name, 'rast'::name);
        $f$, rasterized_census_places, census_place_pop, rasterized_census_places);
END;
$$;