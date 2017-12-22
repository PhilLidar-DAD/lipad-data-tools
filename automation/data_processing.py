import os
import logging
from datetime import datetime
import subprocess
import math
import shutil
import osgeotools

from models import PSQL_DB, Automation_AutomationJob
from utils import assign_status, get_cwd, setup_logging, proper_block_name, proper_block_name_ortho, \
    find_in_coverage
from verify_workers import verify_las, verify_dir


logger = logging.getLogger()

#: Separate logging for renaming tiles
log_msg = []


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
    _TILE_SIZE = 1000
    #: Time data type: Start timing
    startTime = datetime.now()

    #: logger variable for log field in `Automation_AutomationJob`
    logger.info('Renaming tiles ...')
    log_msg.append('Renaming tiles ...\n')

    outDir = outDir.__add__('/' + block_name)
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
        for path, dirs, files in os.walk(inDir, topdown=False):

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
                        minX = float(tokens[1])
                        minY = float(tokens[2])
                        maxX = float(tokens[3])
                        maxY = float(tokens[4])

                        bbox_center_x = (minX + (maxX - minX) / 2)
                        bbox_center_y = (minY + (maxY - minY) / 2)

                        tile_x = int(math.floor(bbox_center_x / float(_TILE_SIZE)))
                        tile_y = int(math.floor(
                            bbox_center_y / float(_TILE_SIZE))) + 1

                        # outFN =
                        # ''.join(['E',tile_x,'N',tile_y,'_',typeFile,'.',typeFile.lower()])
                        outFN = 'E{0}N{1}_{2}_{3}_U{4}.{5}'.format(
                            tile_x, tile_y, typeFile, processor, block_uid, typeFile.lower())
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
                    proj_file = os.path.abspath('/mnt/pmsat-nas_geostorage/DAD/Working/WGS_84_UTM_zone_51N.prj')
                    typeFile = "ORTHO" #tile.split(".")[-1].upper()
                    tile_file_path = os.path.join(path, tile)
                    try:
                        orthophoto, remarks = osgeotools.open_raster(tile_file_path, proj_file)
                        if orthophoto:
                            ul_x = orthophoto["extents"]["min_x"]
                            ul_y = orthophoto["extents"]["max_y"]
                            outFN = 'E{0}N{1}_{2}_{3}_U{4}.{5}'.format(
                                int(ul_x / float(_TILE_SIZE)), int(ul_y / float(_TILE_SIZE)), typeFile, processor, block_uid, typeFile.lower())
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

    if not error:
        assign_status(q, False)
    else:
        assign_status(q, error=True)

    paragraph = ''
    for par in log_msg:
        paragraph = paragraph + par

    #: Save log message from renaming tiles to `Automation_AutomationJob.log`
    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(data_processing_log=paragraph, status_timestamp=datetime.now())
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
    for each_log in log_msg:
        del each_log

    logger.info('Processing Job %s', q.id)

    datatype = q.datatype
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor
    if datatype.lower() == 'laz':
        block_name = proper_block_name(input_dir)
    elif datatype.lower() == 'ortho':
        block_name = proper_block_name_ortho(input_dir)
    if datatype.lower() == 'laz' or datatype.lower() == 'ortho':
        logger.info('Verifying las tiles in directory...')
        log_msg.append('Verifying las tiles in directory...\n')
        has_error, remarks = verify_dir(input_dir, datatype.lower())

        if has_error:
            assign_status(q, error=True)
            log_msg.append('Error in verify_las/verify_raster!\n {0} \n'.format(remarks))
        else:
            logger.info('Renaming tiles...')

            logger.info('BLOCK NAME %s', block_name)
            log_msg.append('BLOCK NAME {0}\n'.format(block_name))

            in_coverage, block_uid = find_in_coverage(block_name)

            #: Check first if folder or `block_name` is in `Cephgeo_LidarCoverageBlock`
            #: If not found, `output_dir` is not created and data is not processed
            if in_coverage:
                logger.info('Found in Lidar Coverage model %s %s',
                            block_name, block_uid)
                log_msg.append('Found in Lidar Coverage model {0} {1}\n'.format(
                               block_name, block_uid))

                rename_tiles(input_dir, output_dir, processor,
                             block_name, block_uid, q)
                logger.info('Status  %s Status Timestamp  %s',
                            q.status, q.status_timestamp)
                log_msg.append('Status  {0} Status Timestamp  {1}\n'.format(
                               q.status, q.status_timestamp))

            else:
                has_error = True
                logger.error('ERROR NOT FOUND IN MODEL %s %s', block_name, block_uid)
                log_msg.append('ERROR NOT FOUND IN MODEL {0} {1}\n'.format(block_name, block_uid))
                assign_status(q, error=True)
    # for DEM
    else:
        logger.info('Handler not implemented for type:  %s',
                    str(q.datatype))
        log_msg.append('Handler not implemented for type:  {0}\n'.format(
                       str(q.datatype)))
        assign_status(q, error=True)

    paragraph = ''
    for par in log_msg:
        paragraph = paragraph + par

    #: Save log messages from renaming tiles to `Automation_AutomationJob.log`
    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(data_processing_log=paragraph, status_timestamp=datetime.now())
                 .where(Automation_AutomationJob.id == q.id))
        new_q.execute()
