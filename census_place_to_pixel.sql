DROP FUNCTION census_place_to_pixel(input_year INT, power FLOAT, smoothing FLOAT, radius1 FLOAT, radius2 FLOAT);
CREATE OR REPLACE FUNCTION census_place_to_pixel(
    year INT,
    power FLOAT DEFAULT 3.0,
    smoothing FLOAT DEFAULT 1.0,
    radius1 FLOAT DEFAULT 10000,
    radius2 FLOAT DEFAULT 10000
)
RETURNS TABLE(val FLOAT, geom POLYGON)
LANGUAGE plpgsql AS
$$
DECLARE
    census_place_pop_count_table TEXT := 'census_place_pop_' || year;
    geo_table TEXT := 'geo_' || year;
    interpolation_settings TEXT := format(
        'invdist:power=%s:smoothing=%s:radius1=%s:radius2=%s',
        power, smoothing, radius1, radius2
    );
BEGIN
RETURN QUERY EXECUTE format($f$
    WITH pop_multipoint AS (
        SELECT ST_Collect(ST_MakePoint(ST_X(ST_Transform(geom::geometry, 5070)), ST_Y(ST_Transform(geom::geometry, 5070)), pop_count::float)) AS geom
        FROM %I -- census_place_pop_count_table
    ),
    pop_sum AS (
        SELECT COUNT(*) AS tot_pop
        FROM %I -- geo_table_name
    ),
    template_raster AS (
        SELECT ST_AddBand(ST_MakeEmptyRaster(rast), 1, '64BF'::text, 0, 0) AS rast
        FROM usa_raster
    ),
    interpolation_raster AS (
        SELECT ST_InterpolateRaster(
           geom,
           %L, -- interpolation_settings
           rast
           ) AS rast
        FROM template_raster, pop_multipoint
    ),
    pixel_raw AS (
        SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
        FROM interpolation_raster
    ),
    val_sum AS (
        SELECT SUM(val) AS tot_val
        FROM pixel_raw
    ),
    pixel_rescaled AS (
        SELECT pp.tot_pop * p.val / vs.tot_val as val, geom
        FROM pixel_raw p
        CROSS JOIN pop_sum pp
        CROSS JOIN val_sum vs
    )
    SELECT val, geom::POLYGON
    FROM pixel_rescaled
    WHERE val > 1;
$f$, census_place_pop_count_table, geo_table, interpolation_settings);
END;
$$;