import argparse
import logging
import math
import os
import osgeotools
import sys
import subprocess


_version = "0.3.5"
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


def _setup_logging_2(logfile, verbosity=None):
    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter("[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d) : %(message)s")

    # Check verbosity for console
    if verbosity and verbosity >= 1:
        global _CONS_LOG_LEVEL
        _CONS_LOG_LEVEL = logging.DEBUG

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Setup file logging
    fh = logging.FileHandler(logfile)
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
    parser.add_argument("-dem_id", "--dem_id", required=True,
                        help="River basin ID for this DEM.")
    parser.add_argument("-l", "--logfile", required=True,
                        help="Filename of logfile")
    args = parser.parse_args()
    return args


def _floor(x):
    return int(math.floor(x / float(_TILE_SIZE)) * _TILE_SIZE)


def _ceil(x):
    return int(math.ceil(x / float(_TILE_SIZE)) * _TILE_SIZE)


def open_dem(src_dem, prj_file, temp_dir):
    # Open DEM raster
    try:
        dem = osgeotools.open_raster(src_dem, prj_file)
    except osgeotools.ProjectionDoesNotMatchError:
        _logger.info('Projection from %s does not match raster dataset!',
                     prj_file)
        _logger.info('Reprojecting DEM...')
        reprojected_dem_path = osgeotools.get_tempfile(temp_dir) + '.tif'
        try:
            gdalwarp = subprocess.check_output(['gdalwarp', '-t_srs',
                                                os.path.abspath(prj_file),
                                                os.path.abspath(src_dem),
                                                reprojected_dem_path])
            _logger.info('Reprojecting DEM...Done!')
        except subprocess.CalledProcessError:
            _logger.exception('Error reprojecting DEM! Exiting.')
            exit(1)
        # Open new DEM
        del dem
        dem = osgeotools.open_raster(reprojected_dem_path, prj_file)

    return dem


def resample_dem(dem, prj_file, temp_dir, tile_extents):
    # Resample DEM to tile extents
    _logger.info("Resampling image...")
    resampled_dem_path = osgeotools.get_tempfile(temp_dir)
    _logger.debug("resample_raster = %s", resampled_dem_path)
    osgeotools.resample_raster(dem, tile_extents, resampled_dem_path)

    # Open resampled DEM
    resampled_dem = osgeotools.open_raster(resampled_dem_path, prj_file)
    _logger.info("Resampled DEM geotransform:\n%s",
                 resampled_dem["geotransform"])
    _logger.info("Resampled DEM extents:\n%s", resampled_dem["extents"])

    return resampled_dem, resampled_dem_path


def generate_tiles(tile_extents, resampled_dem, raster_band, dem_type, dem_id,
                   output_dir, src_dem):
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
            if tile_data is not None:
                ctr = 0
                tile, ul_x, ul_y = tile_data

                # Create new tile geotransform
                tile_gt = list(resampled_dem["geotransform"])
                tile_gt[0], tile_gt[3] = ul_x, ul_y

                # Construct filename
                filename = "E%sN%s_%s_%s.tif" % (tile_x / _TILE_SIZE,
                                                 tile_y / _TILE_SIZE,
                                                 dem_type.upper(),
                                                 dem_id.upper())
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
                                        tile_gt, resampled_dem, raster_band)
                _logger.info(src_dem + ' --------- ' +
                             filename + ' --------- ')

            tile_counter += 1

    _logger.info("Total no. of tiles: {0}".format(tile_counter))


def tile_dem(src_dem, prj_file, temp_dir, out_dir, dem_type, dem_id):
    """
        <src_dem>   - location of source DEM to be tiled
        <out_dir>   - directory to contained the tiled files
        <dem_type>  - DTM or DSM
        <prj_file>  - .prj file for added parameters
        <temp_dir>  - temporary directory to store reporjected/resampled raster
        <log_file>  - log file output
        <_logger>   - python logger instance
    """
    # Open DEM raster
    dem = open_dem(src_dem, prj_file, temp_dir)
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

    resampled_dem, resampled_dem_path = resample_dem(
        dem, prj_file, src_dem, tile_extents)
    _logger.info("Resampled DEM geotransform:\n%s",
                 resampled_dem["geotransform"])

    # Compare rasters
    _logger.info("Comparing rasters...")
    avg, std = osgeotools.compare_rasters(dem, resampled_dem)
    _logger.info("Average difference: %s", avg)
    _logger.info("Standard deviation: %s", std)
    del(dem)

    # Open raster band
    raster_band = osgeotools.open_raster_band(resampled_dem, 1, True)

    # Check if output directory exists
    output_dir = osgeotools.isexists(out_dir)

    # Generate tile upper left coordinates
    generate_tiles(tile_extents, resampled_dem, raster_band, dem_type, dem_id,
                   output_dir, src_dem)

    # Delete temporary resampled DEM
    del resampled_dem
    os.remove(resampled_dem_path)


if __name__ == '__main__':

    # Parse arguments
    args = _parse_arguments()

    # Setup logging
    _setup_logging(args)

    # Tile dem
    tile_dem(args.dem, args.prj_file, args.temp_dir, args.output_dir,
             args.type, args.dem_id)
