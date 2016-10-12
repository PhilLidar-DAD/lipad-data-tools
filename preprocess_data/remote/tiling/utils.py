#!/usr/bin/env python

import os
import subprocess
import osgeotools
import math
import argparse
from datetime import datetime
from osgeo import ogr
import fiona
from shapely.geometry.geo import mapping
from shapely.geometry.polygon import Polygon
import traceback
import logging
import sys


_version = "0.1.18"
print os.path.basename(__file__) + ": v" + _version
_logger = logging.getLogger()
_LOG_LEVEL = logging.DEBUG
_FILE_LOG_LEVEL = logging.WARNING
_CONS_LOG_LEVEL = logging.INFO
_TILE_SIZE = 1000
toplevel_dir = "/mnt/pmsat-nas_geostorage/EXCHANGE/DPC/MISCELLANEOUS/For_FMC/"
output_log_file = "/home/autotiler@ad.dream.upd.edu.ph/fmc_dtms.log"
prj_file = "/home/autotiler@ad.dream.upd.edu.ph/lipad-data-tools/preprocess_data/remote/tiling/WGS_84_UTM_zone_51N.prj"
# from_date = "2016/06/28"
from_date = datetime(2016, 6, 1)
usetime = 'mtime'
# usetime = 'ctime'
print 'usetime:', usetime


def _floor(x):
    return int(math.floor(x / float(_TILE_SIZE)) * _TILE_SIZE)


def _ceil(x):
    return int(math.ceil(x / float(_TILE_SIZE)) * _TILE_SIZE)


def get_dir_size(dir_path='.'):
    cli_cmd = 'du -B1 "' + dir_path + '"'
    # p = subprocess.Popen(cli_cmd.split(
    #     ' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # out, err = p.communicate()
    out = subprocess.check_output(cli_cmd, shell=True)
    return out.split('\t')[0]

def open_postgis_layer(layer_name, postgis_host, postgis_dbname, postgis_user, postgis_password, ):
    source = ogr.Open(("PG:host={0} dbname={1} user={2} password={3}".format(postgis_host,postgis_dbname,postgis_user,postgis_password)))
    pass

def create_box(min_x, min_y, max_x, max_y):
    return Polygon([(min_x, max_y), (min_x, min_y), (max_x, min_y), (max_x, max_y)])
    
def add_dtm_bboxes_from_csv(dtm_csv_file, dtm_shape_file):
    with fiona.open(dtm_shape_file, 'a') as dtm_shp:
        with open(dtm_csv_file, 'r') as dtm_csv:
            feature_count = len(dtm_shp)
            for line in dtm_csv:
                tokens = line.split(',')
                uploaded = tokens[2].strip(' \t\n\r')
                if uploaded == 'NEW':
                    rb_name = tokens[3].strip(' \t\n\r')
                    min_x = int(tokens[5].strip(' \t\n\r'))
                    min_y = int(tokens[6].strip(' \t\n\r'))
                    max_x = int(tokens[7].strip(' \t\n\r'))
                    max_y = int(tokens[8].strip(' \t\n\r'))
                    bbox = create_box(min_x, min_y, max_x, max_y)
                    feature_count += 1
                    print "Writing to shapefile: [{0}, {1}, {2}]".format(feature_count, rb_name, uploaded)
                    dtm_shp.write({
                            #'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                            'geometry': mapping(bbox),
                            'properties': {'gid': feature_count,
                                           'uploaded' : 7,
                                           'rb' : rb_name,
                                           },
                                   })
"""
from utils import find_fmc_dtms
toplevel_dir="/mnt/geostorage/EXCHANGE/DPC/MISCELLANEOUS/For_FMC"
output_log_file="/home/AD/autotiler/fmc_dtms_20160603-2.log"
prj_file="/home/AD/autotiler/scripts/tile_dem/WGS_84_UTM_zone_51N.prj"
from_date="2016/06/02"

find_fmc_dtms(toplevel_dir, output_log_file, prj_file, from_date)

"""

# def find_fmc_dtms(toplevel_dir, output_log_file, prj_file, from_date=None):
#     from_dt = None
#     if from_date is not None:
#         from_dt = datetime.strptime(from_date, "%Y/%m/%d")
#     print "output_log_file:", output_log_file
#     with open(output_log_file, 'w') as log_fh:
#         for dem_dir, dirs, files in os.walk(toplevel_dir):
#             for name in files:
#                 if name == "hdr.adf":

#                     #                time_created = datetime.fromtimestamp(os.path.getctime(dem_dir))
#                     time_modded = datetime.fromtimestamp(
#                         os.path.getmtime(dem_dir))
#                     print 'name:', name, 'time_modded:', time_modded
#                     if from_date is None or time_modded >= from_dt:
#                         print 'YES!!'
#                         time_created = datetime.fromtimestamp(
#                             os.path.getctime(dem_dir)).strftime("%Y/%m/%d")
#                         time_modded = datetime.fromtimestamp(
#                             os.path.getmtime(dem_dir)).strftime("%Y/%m/%d")
#                         # print "[{0}] -  {1}, None".format(time_created,
#                         # dem_dir)
#                         print "[{0} || {1}] -  {2}, None".format(time_created, time_modded, os.path.join(dem_dir, name))
#                         try:
#                             dem = osgeotools.open_raster(dem_dir, prj_file)
#                             tile_extents = [_floor(dem["extents"]["min_x"]),
#                                             _floor(dem["extents"]["min_y"]),
#                                             _ceil(dem["extents"]["max_x"]),
#                                             _ceil(dem["extents"]["max_y"])]
#                             time_created = datetime.fromtimestamp(
#                                 os.path.getctime(dem_dir)).strftime("%Y/%m/%d")
#                             status = "NEW"
#                             rb_name = dem_dir.split(os.sep)[8]
#                             log_fh.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, None\n".format(time_created,
#                                                                                                       dem_dir,
#                                                                                                       status,
#                                                                                                       rb_name,
#                                                                                                       get_dir_size(
#                                                                                                           dem_dir),
#                                                                                                       tile_extents[0], tile_extents[1], tile_extents[2], tile_extents[3]))
#                         except Exception as e:
#                             print 'ERROR!!'
#                             traceback.print_exc()
#                             #log_fh.write("{0}, {1} ERROR, {2}\n".format(time_created, dem_dir, e))
#                             status = "ERROR"
#                             log_fh.write("{0}, {1}, {2}, {3}, {4}, , , , , {5}\n".format(time_created,
#                                                                                          dem_dir,
#                                                                                          status,
#                                                                                          rb_name,
#                                                                                          get_dir_size(
#                                                                                              dem_dir),
# e))


# def test_find_fmc_dtm(toplevel_dir, from_date):
#     for dem_dir, dirs, files in os.walk(toplevel_dir):
#         for name in files:
#             if name == "hdr.adf":
#                 #time_created = datetime.fromtimestamp(os.path.getctime(os.path.join(dem_dir,name))).strftime("%Y/%m/%d")
#                 #time_created = datetime.fromtimestamp(os.path.getctime(os.path.join(dem_dir,name)))
#                 time_created = datetime.fromtimestamp(
#                     os.path.getctime(dem_dir))
#                 from_dt = datetime.strptime(from_date, "%Y/%m/%d")
#                 if time_created > from_dt:
# print "[{0}] -  {1}, None\n".format(time_created, os.path.join(dem_dir))


def find_fmc_dtms(toplevel_dir, prj_file, usetime, from_date=None):
    for root, dirs, files in os.walk(toplevel_dir):
        for f in files:
            f_path = os.path.join(root, f)
            lastdate = datetime.fromtimestamp(
                eval('os.path.get' + usetime + '(f_path)'))
            if (f == 'hdr.adf' and
                (from_date is None or
                    (from_date and lastdate >= from_date))):
                _logger.info('Found! %s: %s', f_path, lastdate)

                # Open DEM
                rb = root.split(os.sep)[8]
                lastdate_str = lastdate.strftime("%Y/%m/%d")
                try:
                    dem = osgeotools.open_raster(root, prj_file)
                    tile_extents = [_floor(dem["extents"]["min_x"]),
                                    _floor(dem["extents"]["min_y"]),
                                    _ceil(dem["extents"]["max_x"]),
                                    _ceil(dem["extents"]["max_y"])]
                    status = 'NEW'

                    _logger.warning('%s,%s,%s,%s,%s,%s,%s,%s,%s', lastdate_str,
                                    root, status, rb, get_dir_size(root),
                                    tile_extents[0], tile_extents[1],
                                    tile_extents[2], tile_extents[3])
                except Exception:
                    _logger.info('Error opening dem!')
                    status = 'ERROR'
                    _logger.warning('%s,%s,%s,%s,%s', lastdate_str, root,
                                    status, rb, get_dir_size(root))

if __name__ == '__main__':

    # Check if file/dirs exists
    if not os.path.isdir(toplevel_dir):
        print toplevel_dir, "doesn't exist! Exiting."
        exit(1)
    if not os.path.isfile(prj_file):
        print prj_file, "doesn't exist! Exiting."
        exit(1)

    # Delete old log file
    if os.path.isfile(output_log_file):
        os.remove(output_log_file)

    # Setup logging
    _logger.setLevel(_LOG_LEVEL)

    # Setup file logging
    fh = logging.FileHandler(output_log_file)
    fh.setLevel(_FILE_LOG_LEVEL)
    fh.setFormatter(logging.Formatter('%(message)s'))
    _logger.addHandler(fh)

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(logging.Formatter("[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d) : %(message)s"))
    _logger.addHandler(ch)

    # find_fmc_dtms(toplevel_dir, prj_file, usetime, from_date)
    find_fmc_dtms(toplevel_dir, prj_file, usetime)
