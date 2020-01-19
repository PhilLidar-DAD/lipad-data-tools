#!/usr/bin/env python2
import os
import logging
from datetime import datetime
import subprocess
import math
import shutil
import osgeotools
import argparse

logger = logging.getLogger()
LOG_LEVEL = logging.INFO

#: Try importing StringIO for logging depending on python version
try:
    from cStringIO import StringIO      # Python 2
except ImportError:
    from io import StringIO

#: Separate logging for renaming tiles
log_msg = []

def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0] + os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0] + os.path.sep

def setup_logging():
    # Setup logging
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
    (%(levelname)s,%(lineno)d)\t: %(message)s')

    # Setup stream logging
    stream = StringIO()
    sh = logging.StreamHandler(stream)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return stream

def rename_tiles(inDir, outDir, processor):
    """Rename each file/tile based on its grid reference.

    A tile is defined is a 1km by 1km division of `LAZ` or `Orthophoto` data. Each
    file is a tile. This function uses `lasbb`, a binary file of `LAStools`. The
    grid reference of a tile is computed using:

        - `minX`: minimum easting coordinate
        - `minY`: minimum northing coordinate
        - maxX`: maximum easting coordinate
        - `maxY`: maximum northing coordinate
        - `bbox_center_x`
        - `bbox_center_y`

    Args:
        inDir (path): Directory containing data tiles to be processed.  This is
        a `block_name ` in the `Cephgeo_LidarCoverageBlock` model.
        outDir (path): The directory where renamed tiles are stored.
        processor (str): Operating System used in processing data.

    Attributes:
        _TILE_SIZE: Tile size in meters, 1000m by 1000m.

    Returns:
        Output directory containing renamed tiles. This functions appends the data
        version at the end of the nameThe final format of a file is:
        **Easting_Northing_FileType_Processor_BlockUID_Version.FileType**

    Raises:
        Warning: A warning is raised if the output directory path already exists.
        Extents Error: If a tile is problematic or corrupted.


    """
    _TILE_SIZE = 1000
    #: Time data type: Start timing
    startTime = datetime.now()

    #: logger variable for log field in `Automation_AutomationJob`
    logger.info('Renaming tiles ...')
    log_msg.append('Renaming tiles ...\n')

    logger.info('Output Directory: %s', outDir)
    log_msg.append('Output Directory: {0}\n'.format(outDir))

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    error = False
    if not os.path.isdir(inDir) and os.listdir(inDir) == []:
        logger.error('Problematic Input Directory %s', inDir)
        log_msg.append('Problematic Input Directory {0}\n'.format(inDir))
        error = True

    if not error:
        #: Loop through the input directory
        for path, dirs, files in os.walk(inDir, topdown=True):

            for tile in files:
                if tile.endswith(".laz"):
                    typeFile = tile.split(".")[-1].upper()
                    ctr = 0
                    tile_file_path = os.path.join(path, tile)

                    #: Get file bounding box/extents
                    p = subprocess.Popen([os.path.join(get_cwd(), 'lasbb'), '-get_bb',
                                          tile_file_path], stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT)
                    out, err = p.communicate()
                    returncode = p.returncode
                    if returncode is 0:
                        tokens = out.split(" ")
                        try:
                            minX = float(tokens[1])
                            minY = float(tokens[2])
                            maxX = float(tokens[3])
                            maxY = float(tokens[4])
                        except:
                            logger.error("Error reading extents of [{0}]. Trace from \
                                lasbb:\n{1}".format(
                                tile_file_path, out))
                            log_msg.append("Error reading extents of [{0}]. Trace from \
                                lasbb:\n{1}\n".format(
                                tile_file_path, out))
                            error = True
                            break
                        bbox_center_x = (minX + (maxX - minX) / 2)
                        bbox_center_y = (minY + (maxY - minY) / 2)

                        tile_x = int(math.floor(bbox_center_x / float(_TILE_SIZE)))
                        tile_y = int(math.floor(
                            bbox_center_y / float(_TILE_SIZE))) + 1

                        # outFN =
                        # ''.join(['E',tile_x,'N',tile_y,'_',typeFile,'.',typeFile.lower()])
                        outFN = 'E{0}N{1}_{2}_{3}.{4}'.format(
                            tile_x, tile_y, typeFile, processor, typeFile.lower())
                        outPath = os.path.join(outDir, outFN)

                        #: Check if output filename is already exists
                        # while os.path.exists(outPath):
                        #     logger.warning('\nWARNING: %s already exists!', outPath)
                        #     log_msg.append('\nWARNING: %s already exists!\n', outPath)
                        #     ctr += 1
                        #     # outFN =
                        #     # ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
                        #     # outFN = 'E{0}N{1}_{2}_{3}_U{4}_{5}.{6}'.format(
                        #     #     tile_x, tile_y, typeFile, processor, block_uid,
                        #     #     str(ctr), typeFile.lower())
                        #     # print outFN
                        #     outPath = os.path.join(outDir, outFN)

                        print 'Path  %s', os.path.join(path, tile), 'Filename: %s', outFN

                        logger.info('%s ---------  %s', os.path.
                                    join(path, tile), outFN)
                        log_msg.append('{0} ---------  {1}\n'.format(os.path.
                                                                     join(path, tile), outFN))

                        # Copy data
                        shutil.copy(tile_file_path, outPath)
                        print outPath, 'Copied success'
                        logger.info('Copied success.')
                        log_msg.append('Copied success.\n')

                    else:
                        logger.error("Error reading extents of [{0}]. Trace from \
                            lasbb:\n{1}".format(
                            tile_file_path, out))
                        log_msg.append("Error reading extents of [{0}]. Trace from \
                            lasbb:\n{1}\n".format(
                            tile_file_path, out))
                        error = True
                        break
                elif tile.endswith(".tif") and 'ortho' in path.lower():
                    proj_file = os.path.abspath(os.path.join(get_cwd(), 'WGS_84_UTM_zone_51N.prj'))
                    typeFile = tile.split(".")[-1].upper()
                    tile_file_path = os.path.join(path, tile)
                    try:
                        orthophoto, remarks = osgeotools.open_raster(tile_file_path, proj_file)
                        if orthophoto:
                            ul_x = orthophoto["extents"]["min_x"]
                            ul_y = orthophoto["extents"]["max_y"]
                            outFN = 'E{0}N{1}_{2}_{3}.{4}'.format(
                                int(ul_x / float(_TILE_SIZE)), int(ul_y / float(_TILE_SIZE)), "ORTHO", processor, typeFile.lower())
                            outPath = os.path.join(outDir, outFN)

                            logger.info('%s ---------  %s', os.path.
                                        join(path, tile), outFN)
                            log_msg.append('{0} ---------  {1}\n'.format(os.path.
                                                                         join(path, tile), outFN))

                            shutil.copy(tile_file_path, outPath)
                            print outPath, 'Copied success'
                            logger.info('Copied success.')
                            log_msg.append('Copied success.\n')
                        else:
                            logger.error("Error for ORTHO [{0}].\n{1}\n".format(
                                tile_file_path,remarks))
                            log_msg.append("Error for ORTHO [{0}].\n{1}\n".format(
                                tile_file_path,remarks))
                            error = True

                    except Exception as e:
                        logger.error("Error for ORTHO [{0}].\n{1}\n".format(
                            tile_file_path,e))
                        log_msg.append("Error for ORTHO [{0}].\n{1}\n".format(
                            tile_file_path,e))
                        error = True
                        break


    endTime = datetime.now()  # End timing
    elapsed_time = endTime - startTime

    logger.info('\nElapsed Time: %s', elapsed_time)
    log_msg.append('Elapsed Time: {0}\n'.format(elapsed_time))

    paragraph = ''
    for par in log_msg:
        paragraph = paragraph + par


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rename LAZ and Orthophotos')
    parser.add_argument('-i','--input_dir')
    parser.add_argument('-p','--processor')
    parser.add_argument('-o','--output_dir')
    args = parser.parse_args()

    setup_logging()
    rename_tiles(args.input_dir, args.output_dir, args.processor)
