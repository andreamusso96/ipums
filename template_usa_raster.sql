DROP FUNCTION IF EXISTS get_template_usa_raster();
CREATE OR REPLACE FUNCTION get_template_usa_raster()
RETURNS raster AS
$$
DECLARE
    usa_raster raster;
BEGIN
    WITH albers_bbox AS (
        SELECT ST_Envelope(ST_Transform(geom, 5070)) AS bbox
        FROM usa_geo
        WHERE gid = 1
    ),
    bbox_params AS (
        SELECT
            ST_Y(ST_PointN(ST_ExteriorRing(bbox), 3)) AS upperlefty,
            ST_X(ST_PointN(ST_ExteriorRing(bbox), 1)) AS upperleftx,
            ST_XMAX(bbox) - ST_XMIN(bbox) AS width_meters,
            ST_YMAX(bbox) - ST_YMIN(bbox) AS height_meters
        FROM albers_bbox
    )
    SELECT ST_AddBand(ST_MakeEmptyRaster(
            width => CAST(1 + width_meters / 1000 AS INTEGER), -- Total width in km
            height => CAST(1 + height_meters / 1000 AS INTEGER), -- Total height in km
            upperleftx => upperleftx,
            upperlefty => upperlefty,
            scalex => 1000,
            scaley => -1000, -- Negative because y decreases as you move down
            skewx => 0,
            skewy => 0,
            srid => 5070
    ), 1, '64BF'::text, 0, 0)
    INTO usa_raster FROM bbox_params;

    RETURN usa_raster;
END;
$$
LANGUAGE plpgsql;