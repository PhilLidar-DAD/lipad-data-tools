import os
import logging
from datetime import datetime
import subprocess
import math
import shutil

from models import PSQL_DB, Automation_AutomationJob
from utils import assign_status, get_cwd, setup_logging, proper_block_name, find_in_coverage


logger = logging.getLogger()

#: Separate logging for renaming tiles
stream = setup_logging()


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

                    logger.info('%s ---------  %s', os.path.
                                join(path, tile), outFN)

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

    if not inDir_error:
        assign_status(q, False)
    else:
        assign_status(q, True)


    #: Save log stream from renaming tiles to `Automation_AutomationJob.log`
    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(data_processing_log=stream.getvalue(), status_timestamp=datetime.now())
                 .where(Automation_AutomationJob.id == q.id))
        new_q.execute()


def process_job(q):
    """Process workers fetched by ORM interface from LiPAD database.

    Check corresponding status of a worker. Get the following attributes of
    ``Automation_AutomationJob`` model object:

        - `status`
        - `status_timestamp`
        - `datatype`
        - `input_dir`
        - `output_dir`
        - `processor`

    Get correct block name from `input_dir`. Find `block_name` in `model`
    ``Cephgeo_LidarCoverageBlock``. If found, do:

        #. Generate `output_dir` with `block_name` as parent directory.
        #. If datatype is LAZ or Orthophoto, rename data tiles if not yet renamed.
        #. Upload data tiles to ``Ceph Object Storage``.
        #. Update and assign status of ``Automation_AutomationJob`` worker
        #.

    Args:
        q (:obj: `Automation_AutomationJob`): A ``Automation_AutomationJob`` object
            fetched from database.

    Raises:
        Exception: If datatype is not in `Automation_AutomationJob.DATATYPE_CHOICES`.

    @TODO:
        User can input a directory containing multiple child directories. Each child
        folder is a `LiDAR coverage block` folder.
    """


    logger.info('Processing Job')

    datatype = q.datatype
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor

    block_name = proper_block_name(input_dir)
    logger.info('BLOCK NAME %s', block_name)

    in_coverage, block_uid = find_in_coverage(block_name)

    #: Check first if folder or `block_name` is in `Cephgeo_LidarCoverageBlock`
    #: If not found, `output_dir` is not created and data is not processed
    if in_coverage:
        logger.info('Found in Lidar Coverage model %s %s',
                    block_name, block_uid)

        #: LAZ and Orthophoto are to be renamed automatically, whether datasets
        #: have been renamed or not
        if datatype.lower() == ('laz' or 'ortho'):
            print 'Will rename tiles ... '
            rename_tiles(input_dir, output_dir, processor,
                         block_name, block_uid, q)
            logger.info('Status  %s Status Timestamp  %s',
                        q.status, q.status_timestamp)
            print '1 Status', q.status, 'Status Timestamp', q.status_timestamp

    # for DEM
        else:
            logger.info('Handler not implemented for type:  %s',
                        str(q.datatype))

        # logger.info('Status  %s Status Timestamp  %s',
        #             q.status, q.status_timestamp)
        # print '2 Status', q.status, 'Status Timestamp', q.status_timestamp

        # # Upload to `Ceph` after processing
        # ceph_uploaded, log_file = ceph_upload(output_dir)
        # if ceph_uploaded:
        #     transfer_metadata(log_file, datatype)
    else:
        logger.error('ERROR NOT FOUND IN MODEL %s %s', block_name, block_uid)


    #: Save log stream from renaming tiles to `Automation_AutomationJob.log`
    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(data_processing_log=stream.getvalue(), status_timestamp=datetime.now())
                 .where(Automation_AutomationJob.id == q.id))
        new_q.execute()

