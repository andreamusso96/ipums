import scipy.ndimage as ndimage
import numpy as np
from raster_postgis import load_raster, dump_raster


def get_2d_exponential_kernel(size: int, decay_rate: float) -> np.ndarray:
    """
    Create a 2D exponential kernel

    Parameters:
    - size: size of the kernel
    - sigma: standard deviation of the kernel

    Returns:
    - A 2D numpy array representing the kernel
    """
    assert size % 2 == 1, "The size of the kernel must be odd"
    indices = np.arange(size) - (size - 1) / 2
    x, y = np.meshgrid(indices, indices)
    distance_grid = np.sqrt(x ** 2 + y ** 2)
    kernel = np.exp(-decay_rate * distance_grid)
    return kernel / np.sum(kernel)


def convolve2d(image, kernel):
    return ndimage.convolve(image, kernel, mode='constant', cval=0.0)


if __name__ == '__main__':
    import psycopg2
    conn = psycopg2.connect("dbname='ipums' user='postgres' host='localhost' password='andrea'")
    raster = load_raster(conn, 'rasterized_census_places1850')
    kernel = get_2d_exponential_kernel(11, 0.2)
    raster_vals = raster.sel(band=1).values
    convolved_raster_vals = convolve2d(raster_vals, kernel)
    convolved_raster = raster.copy(data=np.expand_dims(convolved_raster_vals, axis=0))
    dump_raster(conn, convolved_raster, 'convolved_raster_1850')
