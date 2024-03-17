DROP FUNCTION IF EXISTS convolved_raster_to_cluster(input_year INTEGER, pixel_threshold FLOAT, eps FLOAT, minpoints INT);
CREATE OR REPLACE FUNCTION convolved_raster_to_cluster(
    input_year INTEGER,
    pixel_threshold FLOAT DEFAULT 100,
    eps FLOAT DEFAULT 1,
    minpoints INT DEFAULT 1
)
RETURNS TABLE(cid INT, geom GEOMETRY, pop FLOAT)
LANGUAGE plpgsql AS
$$
DECLARE
    convolved_raster TEXT := 'convolved_raster_' || input_year;
BEGIN
RETURN QUERY EXECUTE format($f$
    WITH pixels AS (
        SELECT (ST_PixelAsPolygons(rast, 1, TRUE)).*
        FROM %I
    ),
    filtered_pixel AS (
        SELECT val, geom
        FROM pixels
        WHERE val > %L -- Pixel threshold
    ),
    dbscan AS (
        SELECT ST_ClusterDBSCAN(geom, eps := %L, minpoints := %L) OVER () AS cid, geom, val
        FROM filtered_pixel
    )
    SELECT cid, ST_Union(geom) AS geom, SUM(val) as pop
    FROM dbscan
    GROUP BY cid
$f$, convolved_raster, pixel_threshold, eps, minpoints);
END
$$;