import scipy.ndimage as ndimage
import numpy as np


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