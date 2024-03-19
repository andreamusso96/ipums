DROP FUNCTION IF EXISTS convolved_raster_to_cluster(convolved_raster_table_name TEXT, pixel_threshold FLOAT, eps FLOAT, minpoints INT);
CREATE OR REPLACE FUNCTION convolved_raster_to_cluster(
    convolved_raster_table_name TEXT,
    pixel_threshold FLOAT DEFAULT 100,
    eps FLOAT DEFAULT 1,
    minpoints INT DEFAULT 1
)
RETURNS TABLE(cid INT, geom GEOMETRY)
LANGUAGE plpgsql AS
$$
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
    SELECT cid, ST_Union(geom) AS geom
    FROM dbscan
    GROUP BY cid
$f$, convolved_raster_table_name, pixel_threshold, eps, minpoints);
END
$$;