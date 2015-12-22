import logging
import numpy
import os
import random
import sys


try:
    from osgeo import ogr, osr, gdal, gdalnumeric, gdalconst
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

_version = "0.1.57"
print os.path.basename(__file__) + ": v" + _version
_logger = logging.getLogger()
_BUFFER = 50  # meters

# Enable GDAL/OGR exceptions
gdal.UseExceptions()


def pixel2world(gt, col_id, row_id):
    x, y = gdal.ApplyGeoTransform(gt, col_id, row_id)
    return x, y


def world2pixel(gt, x, y):
    inv_gt = gdal.InvGeoTransform(gt)[1]
    pixel_loc = gdal.ApplyGeoTransform(inv_gt, x, y)
    col_id, row_id = tuple([int(round(i, 0)) for i in pixel_loc])
    return col_id, row_id


def _image2array(i):
    a = gdalnumeric.fromstring(i.tostring(), 'b')
    a.shape = i.im.size[1], i.im.size[0]
    return a


def isexists(path):
    normpath = os.path.normpath(path)
    # Check if path exists
    if not os.path.exists(normpath):
        _logger.error("%s path does not exist! Exiting.", path)
        exit(1)
    return normpath


def _open_ogr_driver(driver_name):
    # Check if driver exists
    driver = ogr.GetDriverByName(driver_name)
    if driver is None:
        _logger.error("%s driver does not exist! Exiting.", driver_name)
        exit(2)
    return driver


def _open_gdal_driver(driver_name):
    # Check if driver exists
    driver = gdal.GetDriverByName(driver_name)
    if driver is None:
        _logger.error("%s driver does not exist! Exiting.", driver_name)
        exit(2)
    return driver


def open_vector(path, driver_name, mode=gdalconst.GA_ReadOnly):
    isexists(path)
    driver = _open_gdal_driver(driver_name)
    # Open vector
    datasource = driver.Open(path, mode)
    if datasource is None:
        _logger.error("Cannot open %s! Exiting.", path)
        exit(3)
    _logger.info("Opened vector datasource: %s.", path)
    return datasource


def open_raster(path, prj_file):
    name = os.path.basename(isexists(path))
    # Open raster
    dataset = gdal.Open(path)
    if dataset is None:
        _logger.error("Cannot open %s! Exiting.", path)
        exit(4)
    _logger.info("Opened raster dataset: %s.", path)
    raster = {"dataset": dataset,
              "projection": dataset.GetProjection(),
              "geotransform": dataset.GetGeoTransform(),
              "cols": dataset.RasterXSize,
              "rows": dataset.RasterYSize,
              "name": name}
    _logger.debug('raster["cols"] = %s raster["rows"] = %s',
                  raster["cols"], raster["rows"])
    # Also check prjection
    _check_projection(prj_file, raster["dataset"])
    # Compute extents
    # Upper left corner
    min_x, max_y = pixel2world(raster["geotransform"], 0, 0)
    # Lower right corner
    max_x, min_y = pixel2world(raster["geotransform"],
                               raster["cols"], raster["rows"])
    # Construct extents
    extents = {"min_x": min_x,
               "max_x": max_x,
               "min_y": min_y,
               "max_y": max_y}
    raster["extents"] = extents
    return raster


def open_raster_band(raster, bandno, open_band_array=False):
    raster_band = raster["dataset"].GetRasterBand(bandno)
    data = {"nodata": raster_band.GetNoDataValue(),
            "unit_type": raster_band.GetUnitType()}
    if open_band_array:
        _logger.info("Opening raster band of %s...", raster["name"])
        data["band_array"] = raster_band.ReadAsArray()
    _logger.debug('data["nodata"] = %s' % data["nodata"])
    return data


def _check_projection(prj_file, raster_dataset):
    isexists(prj_file)
    # Get prjection from file
    with open(prj_file, 'r') as open_file:
        prj = open_file.read()
    # Convert prjection to spatial reference
    prj_srs = osr.SpatialReference(wkt=prj)
    prj_raster = raster_dataset.GetProjection()
    prj_raster_srs = osr.SpatialReference(wkt=prj_raster)
    # Check if they are the same
    if not prj_srs.IsSame(prj_raster_srs):
        _logger.error("Projection from %s does not match raster dataset! \
Exiting.",
                      prj_file)
        exit(5)
    _logger.info("Projection from %s matches raster dataset.", prj_file)
    return prj


def get_band_array_tile(raster, raster_band, xoff, yoff, size):
    # Assumes a single band raster dataset
    # xoff, yoff - coordinates of upper left corner
    # xsize, ysize - tile size

    # Add buffers
    ul_x, ul_y = xoff - _BUFFER, yoff + _BUFFER
    lr_x, lr_y = xoff + size + _BUFFER, yoff - size - _BUFFER

    # Get tile bounding box pixel coordinates
    ul_c, ul_r = world2pixel(raster["geotransform"], ul_x, ul_y)
    lr_c, lr_r = world2pixel(raster["geotransform"], lr_x, lr_y)
    _logger.debug("ul_c = %s ul_r = %s lr_c = %s lr_r = %s", ul_c, ul_r,
                  lr_c, lr_r)

    # Get tile subset
    tile = raster_band["band_array"][ul_r:lr_r, ul_c:lr_c]

    # Check if band subset has data
    nodata = raster_band["nodata"]
    if nodata == tile.min() == tile.max():
        _logger.debug("Tile has no data! Skipping.")
        return None

    # return tile, tile_cols, tile_rows, ul_x, ul_y
    return tile, ul_x, ul_y


def write_raster(path, driver_name, new_band_array, data_type, geotransform,
                 raster, raster_band):
    # Assumes 1-band raster

    # Check if driver exists
    driver = _open_gdal_driver(driver_name)

    rows, cols = new_band_array.shape

    # Create new raster dataset
    raster_dataset = driver.Create(path, cols, rows, 1, data_type)

    # Set geotransform and prjection of raster dataset
    raster_dataset.SetGeoTransform(geotransform)
    raster_dataset.SetProjection(raster["projection"])

    # Get the first raster band and write the band array data
    raster_dataset.GetRasterBand(1).WriteArray(new_band_array)
    # Also set the no data value, unit type and compute statistics
    raster_dataset.GetRasterBand(1).SetNoDataValue(raster_band["nodata"])
    raster_dataset.GetRasterBand(1).SetUnitType(raster_band["unit_type"])
    raster_dataset.GetRasterBand(1).ComputeStatistics(False)
    # Flush data
    del raster_dataset


def resample_raster(raster, extents, resampled_path):
    # Assuming 1-band raster

    # Get new raster extents
    ul_x, ul_y = extents["min_x"], extents["max_y"]
    lr_x, lr_y = extents["max_x"], extents["min_y"]
    # Add buffers
    ul_x, ul_y = ul_x - _BUFFER, ul_y + _BUFFER
    lr_x, lr_y = lr_x + _BUFFER, lr_y - _BUFFER
    _logger.debug(
        "ul_x = %s ul_y = %s lr_x = %s lr_y = %s", ul_x, ul_y, lr_x, lr_y)
    # Get new geotransform
    gt = list(raster["geotransform"])
    gt[0], gt[3] = ul_x, ul_y
    _logger.debug("gt = %s", gt)
    # Get new raster size
    lr_c, lr_r = world2pixel(gt, lr_x, lr_y)
    _logger.debug("lr_c = %s lr_r = %s", lr_c, lr_r)
    # Create new raster
    driver = _open_gdal_driver("GTiff")
    resampled = driver.Create(resampled_path, lr_c, lr_r, 1,
                              gdalconst.GDT_Float32)
    # Set new raster properties
    prj = raster["projection"]
    resampled.SetGeoTransform(gt)
    resampled.SetProjection(prj)
    # Set new raster band data
    raster_band = open_raster_band(raster, 1)
    nodata = raster_band["nodata"]
    unit_type = raster_band["unit_type"]
    resampled.GetRasterBand(1).SetNoDataValue(nodata)
    resampled.GetRasterBand(1).Fill(nodata)
    resampled.GetRasterBand(1).SetUnitType(unit_type)
    # Resample image
    gdal.ReprojectImage(raster["dataset"], resampled, prj, prj,
                        gdalconst.GRA_NearestNeighbour)
    # Compute statistics
    resampled.GetRasterBand(1).ComputeStatistics(False)
    # Flush data
    del resampled


def _estimate_sample_size(N):
    # Sources:
    # http://www.surveysystem.com/sample-size-formula.htm
    # http://www.raosoft.com/samplesize.html
    #
    # N = population
    # Confidence level
    c = .99
    # Critical value/standard score
    import scipy.stats as st
    a = 1 - c
    _logger.debug("a = %s", a)
    z = st.norm.ppf(1 - a / 2.)
    _logger.debug("z = %s", z)
    # Response distribution(?)
    r = .5
    # Margin of error/confidence interval
    e = .01
    # Compute sample size
    x = (z ** 2) * r * (1 - r)
    _logger.debug("x = %s", x)
    n = (N * x) / ((N - 1) * (e ** 2) + x)
    _logger.debug("n = %s", n)

    return int(n)


def compare_rasters(src_raster, dst_raster):
    _logger.info("Computing average difference...")
    # Compute sample size
    pop_size = src_raster["rows"] * src_raster["cols"]
    # Comma-separated integer?
    _logger.info("Total no. of pixels: %s", pop_size)
    sample_size = _estimate_sample_size(pop_size)
    _logger.info("Sample size: %s", sample_size)
    # Open raster bands (assuming 1-band raster)
    src_raster_band = open_raster_band(src_raster, 1, True)
    dst_raster_band = open_raster_band(dst_raster, 1, True)
    src_band_array = src_raster_band["band_array"]
    dst_band_array = dst_raster_band["band_array"]
    # Get random samples
    rng = random.SystemRandom()
    samples = numpy.zeros(sample_size, dtype=src_band_array.dtype)
    pixel_locs = set()
    last_pct = 0
    for i in xrange(sample_size):
        # Get an unused random pixel location
        while True:
            col_id = rng.randint(0, src_raster["cols"] - 1)
            row_id = rng.randint(0, src_raster["rows"] - 1)
            pixel_loc = row_id, col_id
            src_value = src_band_array[pixel_loc]
            if (not pixel_loc in pixel_locs and
                    src_value != src_raster_band["nodata"]):
                pixel_locs.add(pixel_loc)
                break
        # Get value in destination raster
        src_x, src_y = pixel2world(src_raster["geotransform"], col_id, row_id)
        dst_c, dst_r = world2pixel(dst_raster["geotransform"], src_x, src_y)
        dst_value = dst_band_array[dst_r, dst_c]
        # Compute difference
        samples[i] = src_value - dst_value
        # Display progress
        cur_pct = int(i / float(sample_size) * 100)
        if cur_pct % 10 == 0 and cur_pct > last_pct:
            _logger.info("Progress: {0}%".format(cur_pct))
            last_pct = cur_pct
    # Compute average and standard deviation
    avg = numpy.average(samples)
    std = numpy.std(samples)
    return avg, std
