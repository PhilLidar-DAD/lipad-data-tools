import os
import osgeotools
import math
import argparse
from datetime import datetime
_TILE_SIZE = 1000

_version = "0.1.0"

def _floor(x):
    return int(math.floor(x / float(_TILE_SIZE)) * _TILE_SIZE)

def _ceil(x):
    return int(math.ceil(x / float(_TILE_SIZE)) * _TILE_SIZE)

def _parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Finds \
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
    args = parser.parse_args()
    return args


def find_fmc_dtms(toplevel_dir, output_log_file, prj_file):
    with open(output_log_file, 'w') as log_fh:
        for dem_dir, dirs, files in os.walk(toplevel_dir):
            for name in files:
                if name == "hdr.adf":
                    try:
                        dem = osgeotools.open_raster(dem_dir, prj_file)
                        tile_extents = [_floor(dem["extents"]["min_x"]),
                                        _floor(dem["extents"]["min_y"]),
                                        _ceil(dem["extents"]["max_x"]),
                                        _ceil(dem["extents"]["max_y"])]
                        time_created = datetime.fromtimestamp(os.path.getctime(dem_dir)).strftime("%Y/%m/%d")
                        log_fh.write("{0}, {1}, {2}, None\n".format(time_created, dem_dir, tile_extents))
                    except Exception as e:
                        log_fh.write("{0}, {1} ERROR, {2}\n".format(time_created, dem_dir, e))

if __name__ == '__main__':
    args = _parse_arguments()