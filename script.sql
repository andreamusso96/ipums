DROP PROCEDURE  process_census_data(year INTEGER, interpolation_power DOUBLE PRECISION, interpolation_smoothing DOUBLE PRECISION, interpolation_radius1 DOUBLE PRECISION, interpolation_radius2 DOUBLE PRECISION, dbscan_eps DOUBLE PRECISION, dbscan_min_points INTEGER, cluster_pixel_threshold INTEGER);
CREATE OR REPLACE PROCEDURE process_census_data(
    year INTEGER,
    interpolation_power DOUBLE PRECISION,
    interpolation_smoothing DOUBLE PRECISION,
    interpolation_radius1 DOUBLE PRECISION,
    interpolation_radius2 DOUBLE PRECISION,
    dbscan_eps DOUBLE PRECISION,
    dbscan_min_points INTEGER,
    cluster_pixel_threshold INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if usa_raster table exists, if not, create it
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'usa_raster') THEN
        CREATE TABLE usa_raster(rast raster);
        INSERT INTO usa_raster(rast)
        SELECT create_usa_raster();
        SELECT AddRasterConstraints('usa_raster'::name, 'rast'::name);
    END IF;

    -- Processing census place population for the given year
    EXECUTE FORMAT('DROP TABLE IF EXISTS census_place_pop_%s', year);
    EXECUTE FORMAT('CREATE TABLE census_place_pop_%s(pop_count INTEGER, geom GEOGRAPHY)', year);
    EXECUTE FORMAT('INSERT INTO census_place_pop_%s SELECT * FROM get_census_place_pop_count(%L)', year, year);

    -- Convert census data to pixels
    EXECUTE FORMAT('DROP TABLE IF EXISTS pixel_%s', year);
    EXECUTE FORMAT('CREATE TABLE pixel_%s(val FLOAT, geom POLYGON)', year);
    EXECUTE FORMAT('INSERT INTO pixel_%s SELECT * FROM census_place_to_pixel(%L, %L, %L, %L, %L)',
        year, year, interpolation_power, interpolation_smoothing, interpolation_radius1, interpolation_radius2);

    -- Create clusters from pixels
    EXECUTE FORMAT('DROP TABLE IF EXISTS cluster_%s_v2', year);
    EXECUTE FORMAT('CREATE TABLE cluster_%s_v2(cid INTEGER, geom GEOMETRY)', year);
    EXECUTE FORMAT('INSERT INTO cluster_%s_v2 SELECT * FROM pixel_to_cluster(%L, %L, %L, %L)',
        year, year, cluster_pixel_threshold, dbscan_eps, dbscan_min_points);

    -- Cleanup, if necessary
    -- DROP FUNCTION IF EXISTS census_place_to_pixel(year INTEGER, power DOUBLE PRECISION, smoothing DOUBLE PRECISION, radius1 DOUBLE PRECISION, radius2 DOUBLE PRECISION);
END;
$$;
