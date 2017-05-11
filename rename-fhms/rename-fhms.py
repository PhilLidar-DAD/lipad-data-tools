#!/usr/bin/env python2

"""
Description:

This script renames FHM shapefiles based on their previous filename in the
FHM Coverage shapefile.

Run using ArcGIS Python.

Usage example:

# Test run
C:\Python27_ArcGIS10.3\ArcGIS10.3\python.exe rename-fhms.py \
-fc fhm_coverage_20170511.shp \
-s Q:\DAD\FLOOD_HAZARD\Flood_Hazard_Shapefiles\QCed_FHMs\ \
-d Q:\DAD\FLOOD_HAZARD\Flood_Hazard_Shapefiles\rename_test\ \
-fco RBFP_shp \
-fcn RBFP_name \
-v

# Rename
C:\Python27_ArcGIS10.3\ArcGIS10.3\python.exe rename-fhms.py \
-fc fhm_coverage_20170511.shp \
-s Q:\DAD\FLOOD_HAZARD\Flood_Hazard_Shapefiles\QCed_FHMs\ \
-d Q:\DAD\FLOOD_HAZARD\Flood_Hazard_Shapefiles\rename_test\ \
-fco RBFP_shp \
-fcn RBFP_name \
-v -r

License:

Copyright (c) 2017, Kenneth Langga (klangga@gmail.com)
All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""


import logging
import argparse
import os
import sys
import arcpy
import re
import errno
import glob

logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.ERROR
FILE_LOG_LEVEL = logging.INFO


def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action="store_true")
    parser.add_argument('-r', '--rename', action="store_true")
    parser.add_argument('-s', '--src', required=True)
    parser.add_argument('-d', '--dst', required=True)
    parser.add_argument('-fc', '--fhm-coverage', required=True)
    parser.add_argument('-fco', '--fc-old-field', required=True)
    parser.add_argument('-fcn', '--fc-new-field', required=True)
    args = parser.parse_args()
    return args


def setup_logging(args):
    # Setup logging
    logger.setLevel(LOG_LEVEL)

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)

    if args.verbose:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(CONS_LOG_LEVEL)
    ch_formatter = logging.Formatter(
        '[%(asctime)s] (%(levelname)s) : %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)

    # Setup file logging
    fh = logging.FileHandler(os.path.splitext(
        os.path.basename(__file__))[0] + '.log', mode='w')
    fh.setLevel(FILE_LOG_LEVEL)
    fh_formatter = logging.Formatter(
        '%(message)s')
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)


def check_if_paths_exist(args):
    src_path = os.path.abspath(args.src)
    if not os.path.isdir(src_path):
        logger.error('src: %s does not exist! Exiting.', src_path)
        exit(1)
    dst_path = os.path.abspath(args.dst)
    if not os.path.isdir(dst_path):
        logger.error(
            'dst: %s does not exist! Please create it first. Exiting.',
            dst_path)
        exit(1)
    fhm_coverage_path = os.path.abspath(args.fhm_coverage)
    if not os.path.isfile(fhm_coverage_path):
        logger.error('fhm_coverage: %s does not exist! Exiting.')
        exit(1)
    return src_path, dst_path, fhm_coverage_path


def get_filename_rbfp_map(fhm_coverage):
    arcpy.MakeFeatureLayer_management(fhm_coverage['path'], 'fhm_coverage_lyr')
    filename_rbfp_map = {}
    with arcpy.da.SearchCursor('fhm_coverage_lyr',
                               [fhm_coverage['old'],
                                fhm_coverage['new']]) as cursor:
        for row in cursor:
            # logger.debug('row: %s', row)
            filename_rbfp_map[row[0]] = row[1]
    arcpy.Delete_management('fhm_coverage_lyr')
    return filename_rbfp_map


def find_fhm_shapefiles(src_path, dst_basepath, filename_rbfp_map,
                        rename=False):
    # Find shapefiles in the dir and subdirs
    for root, dirs, files in os.walk(src_path):
        # Ignore hidden dirs
        dirs[:] = sorted([d for d in dirs if not d[0] == '.'])
        # List all files that end with .shp
        for filename in sorted(files):
            fn_lower = filename.lower()
            if fn_lower.endswith('.shp'):
                # Get filename elements
                file_path = os.path.join(root, filename)
                year = get_year(fn_lower, file_path)
                if year is None:
                    # Skip
                    continue
                res = get_resolution(fn_lower)
                fn_head, fn_ext = os.path.splitext(filename)
                rbfp_name = get_rbfp_name(fn_head, year,
                                          filename_rbfp_map, file_path)
                if rbfp_name is None:
                    # Skip
                    continue
                # Construct new filename head and dest. path
                new_fn_head = rbfp_name + '_fh' + year + 'yr_' + res
                # logger.debug('%s %s', dst_basepath,
                #              root.replace(src_path + os.sep, ''))
                subdir = (root
                          .replace(src_path + os.sep, '')
                          .replace(' ', '_')
                          .lower())
                dst_path = os.path.join(dst_basepath, subdir)
                # Rename shapefile
                rename_shapefile(root, fn_head, dst_path, new_fn_head, rename)
                # logger.debug('%s,%s,%s,%s,%s,%s,%s',
                #              file_path, year, res, fn_head, rbfp_name,
                #              new_fn_head, dst_path)
                # exit(1)


def get_year(fn_lower, file_path):
    # Get year
    match_year = re.search(r'(5|25|100)', fn_lower)
    if match_year:
        year = match_year.group(1)
        # logger.info('year: %s', year)
        return year
    else:
        logger.error('Year not found: %s!', file_path)
        # exit(1)


def get_resolution(fn_lower):
    # Get resolution
    if ('10m' in fn_lower and '30m' in fn_lower):
        res = '30m_10m'
    elif '10m' in fn_lower:
        res = '10m'
    elif '30m' in fn_lower:
        res = '30m'
    else:
        res = '10m'
    return res


def get_rbfp_name(fn_head, year, filename_rbfp_map, file_path):
    # Get 100yr filename
    fn_100yr = fn_head.replace(year, '100')
    # logger.debug('fn_100yr: %s', fn_100yr)
    rbfp_name = None
    try:
        rbfp_name = filename_rbfp_map[fn_100yr].lower()
    except KeyError:
        logger.error('No rbfp name for: fn_100yr=%s file_path=%s',
                     fn_100yr, file_path)
    return rbfp_name


def rename_shapefile(src_path, old_fn_head, dst_path, new_fn_head, rename):
    # Create dst_path directories
    if rename:
        mkdir_p(dst_path)

    # Copy and rename shapefiles
    for src_file in glob.glob(os.path.join(src_path, old_fn_head) + '*'):
        # logger.debug('src_file: %s', src_file)
        old_filename = os.path.basename(src_file)
        # logger.debug('old_filename: %s', old_filename)
        new_filename = old_filename.replace(old_fn_head, new_fn_head)
        # logger.debug('new_filename: %s', new_filename)
        dst_file = os.path.join(dst_path, new_filename)
        if rename:
            # Delete dest. file if it exists
            if os.path.isfile(dst_file):
                os.remove(dst_file)
            os.rename(src_file, dst_file)
        logger.info('%s,%s', src_file, dst_file)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def main():

    # Parse arguments
    # logger.debug('parsing arguments...')
    args = parse_arguments()

    # Setup logging
    setup_logging(args)
    logger.debug('args: %s', args)

    # Check if paths exist
    logger.debug('checking if paths exist...')
    src_path, dst_path, fhm_coverage_path = check_if_paths_exist(args)
    logger.debug('src_path: %s', src_path)
    logger.debug('dst_path: %s', dst_path)
    logger.debug('fhm_coverage_path: %s', fhm_coverage_path)

    # Get old filename/rbfp name mapping
    fhm_coverage = {'path': fhm_coverage_path,
                    'old': args.fc_old_field,
                    'new': args.fc_new_field}
    logger.debug('getting old filename/rbfp name mapping...')
    filename_rbfp_map = get_filename_rbfp_map(fhm_coverage)

    # Find FHM shapefiles
    if args.rename:
        logger.debug('renaming fhm shapefiles...')
    else:
        pass
    find_fhm_shapefiles(src_path, dst_path, filename_rbfp_map, args.rename)


if __name__ == "__main__":
    main()
