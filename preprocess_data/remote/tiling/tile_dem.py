#!/usr/bin/python2.7

import argparse
import logging
import math
import os
import osgeotools
import random
import string
import sys


_version = "0.3.2"
print os.path.basename(__file__) + ": v" + _version
_logger = logging.getLogger()
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG
_TILE_SIZE = 1000
_BUFFER = 50  # meters


def _setup_logging(args):
    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter("[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d) : %(message)s")

    # Check verbosity for console
    if args.verbose and args.verbose >= 1:
        global _CONS_LOG_LEVEL
        _CONS_LOG_LEVEL = logging.DEBUG

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Setup file logging
    fh = logging.FileHandler(args.logfile)
    fh.setLevel(_FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    _logger.addHandler(fh)


def _parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Generates 1k x 1k GeoTIFF \
tiles from input DEM.",
                                     epilog="Example: ./tile_dem.py \
-d data/MINDANAO1/d_mdn/ -t dsm -p WGS_84_UTM_zone_51N.prj -o output/")
    parser.add_argument("--version", action="version",
                        version=_version)
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-d", "--dem", required=True,
                        help="Path to the DEM directory/file.")
    parser.add_argument("-t", "--type", choices=["dsm", "dtm"], required=True,
                        help="If DEM is a DSM/DTM.")
    parser.add_argument("-p", "--prj_file", required=True,
                        help="Path to the projection file. Checks if the DEM's \
projection is the same.")
    parser.add_argument("-o", "--output-dir", required=True,
                        help="Path to output directory.")
    parser.add_argument("-tmp", "--temp-dir", required=True,
                        help="Path to temporary working directory.")
    parser.add_argument("-l", "--logfile", required=True,
                        help="Filename of logfile")
    args = parser.parse_args()
    return args


def _floor(x):
    return int(math.floor(x / float(_TILE_SIZE)) * _TILE_SIZE)


def _ceil(x):
    return int(math.ceil(x / float(_TILE_SIZE)) * _TILE_SIZE)

if __name__ == '__main__':

    # Parse arguments
    args = _parse_arguments()

    # Setup logging
    _setup_logging(args)

    # Open DEM raster
    dem = osgeotools.open_raster(args.dem, args.prj_file)

    _logger.info("Current DEM geotransform:\n%s", dem["geotransform"])
    _logger.info("Current DEM extents:\n%s", dem["extents"])

    # Get tile extents
    # Basically, the extents (to the nearest 1k) of the bounding box that
    # encompasses the raster DEM.
    tile_extents = {"min_x": _floor(dem["extents"]["min_x"]),
                    "max_x": _ceil(dem["extents"]["max_x"]),
                    "min_y": _floor(dem["extents"]["min_y"]),
                    "max_y": _ceil(dem["extents"]["max_y"])}
    _logger.info("Tile extents:\n%s", tile_extents)

    # Resample DEM to tile extents
    _logger.info("Resampling image...")
    # Get a temporary file for resampled raster
    random_string = ''.join(random.choice(string.ascii_lowercase +
                                          string.digits) for _ in range(16))
    temp_dir = osgeotools.isexists(args.temp_dir)
    resampled_dem_path = os.path.join(temp_dir,
                                      "tile_dem_tmp_" + random_string)
    _logger.debug("resample_raster = %s", resampled_dem_path)
    osgeotools.resample_raster(dem, tile_extents, resampled_dem_path)

    # Open resampled DEM
    resampled_dem = osgeotools.open_raster(resampled_dem_path, args.prj_file)
    _logger.info("Resampled DEM geotransform:\n%s",
                 resampled_dem["geotransform"])
    _logger.info("Resampled DEM extents:\n%s", resampled_dem["extents"])

    # Compare rasters
    _logger.info("Comparing rasters...")
    avg, std = osgeotools.compare_rasters(dem, resampled_dem)
    _logger.info("Average difference: %s", avg)
    _logger.info("Standard deviation: %s", std)

    # Open raster band
    raster_band = osgeotools.open_raster_band(resampled_dem, 1, True)

    # Check if output directory exists
    output_dir = osgeotools.isexists(args.output_dir)

    # Generate tile upper left coordinates
    _logger.info("Generating tiles...")
    tile_counter = 0
    for tile_y in xrange(tile_extents["min_y"] + _TILE_SIZE,
                         tile_extents["max_y"] + _TILE_SIZE,
                         _TILE_SIZE):
        for tile_x in xrange(tile_extents["min_x"],
                             tile_extents["max_x"],
                             _TILE_SIZE):

            # Get tile of band array
            tile_data = osgeotools.get_band_array_tile(resampled_dem,
                                                       raster_band,
                                                       tile_x, tile_y,
                                                       _TILE_SIZE)

            # If tile has data
            if not tile_data is None:
                ctr = 0
                tile, ul_x, ul_y = tile_data

                # Create new tile geotransform
                tile_gt = list(dem["geotransform"])
                tile_gt[0], tile_gt[3] = ul_x, ul_y

                # Construct filename
                filename = "E%sN%s_%s.tif" % (tile_x / _TILE_SIZE,
                                              tile_y / _TILE_SIZE,
                                              args.type.upper())
                tile_path = os.path.join(output_dir, filename)

                # Check if output filename is already exists
                while os.path.exists(tile_path):
                    print '\nWARNING:', tile_path, 'already exists'
                    ctr += 1
                    filename = filename.replace(
                        '.tif', '_' + str(ctr) + '.tif')
                    tile_path = os.path.join(output_dir, filename)

                # Save new GeoTIFF
                osgeotools.write_raster(tile_path, "GTiff", tile,
                                        osgeotools.gdalconst.GDT_Float32,
                                        tile_gt, dem, raster_band)
                _logger.info(args.dem + ' --------- ' + filename + ' --------- ')

            tile_counter += 1

        # exit(1)
    _logger.info("Total no. of tiles: {0}".format(tile_counter))

    # Delete temporary resampled DEM
    resampled_dem = None
    os.remove(resampled_dem_path)
