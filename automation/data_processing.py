import os
import logging
from datetime import datetime
import subprocess
import math
import shutil

from .models import PSQL_DB, Automation_AutomationJob
from .utils import assign_status, get_cwd, setup_logging


logger = logging.getLogger()


def rename_tiles(inDir, outDir, processor, block_name, block_uid, q):
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
        block_uid (int): Corresponding `uid` of the `block_name`. This `uid` is from
            `Cephgeo_LidarCoverageBlock` model.

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

    #: Time data type: Start timing
    startTime = datetime.now()

    #: Separate logging for renaming tiles
    stream = setup_logging()

    #: logger variable for log field in `Automation_AutomationJob`
    logger.info('Renaming tiles ...')

    outDir = outDir.__add__('/' + block_name)
    logger.info('Output Directory: %s', outDir)

    if not os.path.exists(outDir):
        os.makedirs(outDir)

    inDir_error = False
    if not os.path.isdir(inDir) and os.listdir(inDir) == []:
        logger.error('Problematic Input Directory %s', inDir)
        inDir_error = True

    #: Loop through the input directory
    for path, dirs, files in os.walk(inDir, topdown=False):

        for tile in files:
            if tile.endswith(".laz") or tile.endswith(".tif"):
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
                    minX = float(tokens[1])
                    minY = float(tokens[2])
                    maxX = float(tokens[3])
                    maxY = float(tokens[4])

                    bbox_center_x = (minX + (maxX - minX) / 2)
                    bbox_center_y = (minY + (maxY - minY) / 2)

                    _TILE_SIZE = 1000
                    tile_x = int(math.floor(bbox_center_x / float(_TILE_SIZE)))
                    tile_y = int(math.floor(
                        bbox_center_y / float(_TILE_SIZE))) + 1

                    # outFN =
                    # ''.join(['E',tile_x,'N',tile_y,'_',typeFile,'.',typeFile.lower()])
                    outFN = 'E{0}N{1}_{2}_{3}_U{4}.{5}'.format(
                        tile_x, tile_y, typeFile, processor, block_uid, typeFile.lower())
                    outPath = os.path.join(outDir, outFN)

                    #: Check if output filename is already exists
                    while os.path.exists(outPath):
                        logger.warning('\nWARNING: %s already exists!', outPath)
                        ctr += 1
                        # outFN =
                        # ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
                        outFN = 'E{0}N{1}_{2}_{3}_U{4}_{5}.{6}'.format(
                            tile_x, tile_y, typeFile, processor, block_uid,
                            str(ctr), typeFile.lower())
                        # print outFN
                        outPath = os.path.join(outDir, outFN)

                    print 'Path  %s', os.path.join(path, tile), 'Filename: %s', outFN
                    logger.info('Path %s Filename: %s', os.path.join(
                        path, tile), outFN)

                    logger.info('Path + Filename  %s ---------  %s\n', os.path.
                                join(path, tile), outFN)

                    logger.info(' %s Filename okay. Wont copy data yet', outPath)
                    # Copy data
                    shutil.copy(tile_file_path, outPath)
                    print outPath, 'Copied success'
                    logger.info('Copied success.')
                else:
                    logger.error("Error reading extents of [{0}]. Trace from \
                        lasbb:\n{1}".format(
                        tile_file_path, out))

    endTime = datetime.now()  # End timing
    elapsed_time = endTime - startTime

    logger.info('\nElapsed Time: %s', elapsed_time)

    print '#' * 40
    print 'Stream value', stream.getvalue()
    print '#' * 40

    #: Save log stream from renaming tiles to `Automation_AutomationJob.log`
    if not inDir_error:
        assign_status(q)

    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(log=stream.getvalue(), status_timestamp=datetime.now())
                 .where(Automation_AutomationJob.id == q.id))
        new_q.execute()
