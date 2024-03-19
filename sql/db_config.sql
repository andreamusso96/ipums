CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
ALTER DATABASE ipums SET postgis.gdal_enabled_drivers = 'ENABLE_ALL';
