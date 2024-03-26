from rasterio.io import MemoryFile
import rioxarray as riox
import xarray as xr


def load_raster(con, raster_table: str, raster_column: str = 'rast') -> xr.DataArray:
    """
    Load a specific a PostGIS raster into a rioxarray DataArray

    Parameters:
    - conn: psycopg2 connection object to the database
    - raster_table: Name of the table containing the raster
    - raster_column: Name of the column containing the raster

    Returns:
    - A rioxarray DataArray object representing the raster
    """

    with con.cursor() as cursor:
        cursor.execute(f"SELECT ST_AsGDALRaster({raster_column}, 'GTIff') FROM {raster_table}")
        raster = cursor.fetchone()

    in_memory_raster = MemoryFile(bytes(raster[0]))
    raster_dataset = riox.open_rasterio(in_memory_raster)
    return raster_dataset


def dump_raster(con, data: xr.DataArray, table_name:str):
    """
    Dump a rioxarray DataArray into a PostGIS raster table

    :param con: psycopg2 connection object to the database
    :param data: a rioxarray DataArray object representing the raster
    :param table_name: Name of the table to store the raster (it must not exist)
    :return: None

    """
    assert data.rio is not None, "The input data must be a rioxarray DataArray"
    assert data.rio.crs is not None, "The input data must have a CRS"
    assert data.rio.transform() is not None, "The input data must have a transform"

    raster_array = data.rio
    width, height = raster_array.width, raster_array.height
    bands = raster_array.count
    srid = raster_array.crs.to_epsg()
    nodata = raster_array.nodata
    with MemoryFile() as memory_file:
        with memory_file.open(driver='GTiff', width=width, height=height, count=bands, dtype=raster_array._obj.dtype, crs=f'EPSG:{srid}', transform=raster_array.transform(), nodata=nodata) as dataset:
            dataset.write(data)

        geotiff_data = memory_file.read()

    with con.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (rast raster);")
        cursor.execute(f"INSERT INTO {table_name} (rast) VALUES (ST_FromGDALRaster(%s))", (geotiff_data,))
        cursor.execute(f"SELECT AddRasterConstraints('{table_name}'::name, 'rast'::name);")
        con.commit()

