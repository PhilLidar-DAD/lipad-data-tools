import os
from models import *
import random
import subprocess
from datetime import datetime
import logging
import math
import shutil
import logging


logger = logging.getLogger()
LOG_LEVEL = logging.INFO

#: Try importing StringIO for logging depending on python version
try:
    from cStringIO import StringIO      # Python 2
except ImportError:
    from io import StringIO


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


def ceph_upload(input_dir_ceph):
    # @TODO
    # Separate logging field in Automation Model
    stream = setup_logging()

    print 'Uploading to Ceph ....'
    logger.info('Uploading to Ceph ....')

    try:
        output = subprocess.check_output(
            ['./bulk_upload_nonthreaded.py', input_dir_ceph])

        print '#' * 40
        print 'Ceph Output ... '
        logger.info('Ceph Output ... ')
        print '#' * 40

        print output, len(output)
        logger.info('%s %s', output, len(output))

        filename = output.split('\n')[-1]
        print 'Logfile', filename
        logger.info('Logfile %s', filename)

        if 'Done Uploading!' in output:
            print 'Caught Done Uploading!'
        return True, filename

    except Exception:
        print 'Error in Ceph upload!'
        return False, None


def tile_dtm(dem_dir, output_dir):
    """./tile_dem.py -d data/MINDANAO1/v_mdn/ -t dtm -p WGS_84_UTM_zone_51N.prj -o output/"""

    print 'Tiling DTM'

    dtm_dir = [name for name in os.listdir(dem_dir) if (
        "cv_" in name or "uv_" in name) and os.path.isdir(os.path.join(dem_dir, name))][0]

    try:
        output = subprocess.check_output(
            ['./tile_dem.py',
                '-d', dtm_dir,
                '-t', 'dtm',
                '-p', 'WGS_84_UTM_zone_51N.prj',
                '-o', os.path.join(output_dir, 'DTM'),
             ])

        if 'Done Tiling' in output:
            print 'DTM Tiling Done!'
        return True
    except Exception:
        print 'Error in DTM tiling. DTM directory: ' + dtm_dir
        return False


def tile_dsm(dem_dir, output_dir):
    """./tile_dem.py -d data/MINDANAO1/d_mdn/ -t dsm -p WGS_84_UTM_zone_51N.prj -o output/"""

    print 'Tiling DSM'

    dsm_dir = [name for name in os.listdir(dem_dir) if (
        "cd_" in name or "ud_" in name) and os.path.isdir(os.path.join(dem_dir, name))][0]

    try:
        output = subprocess.check_output(
            ['./tile_dem.py',
                '-d', dsm_dir,
                '-t', 'dsm',
                '-p', 'WGS_84_UTM_zone_51N.prj',
                '-o', os.path.join(output_dir, 'DSM'),
             ])

        if 'Done Tiling' in output:
            print 'DSM Tiling Done!'
        return True
    except Exception:
        print 'Error in DSM tiling. DSM directory: ' + dsm_dir
        return False, None


def get_delay(min_, max_):
    return float('%.2f' % random.uniform(min_, max_))


def assign_status(q, status_):
    status = q.status
    print 'Status:', status
    loggger.info('Status: %s', status)

    for i in [i for i, x in enumerate(Automation_AutomationJob.STATUS_CHOICES)
              if x == status]:
        status += 1

    if status == 1:
        new_status = 'done_process'
        logger.info('Processed Job')
    elif status == 2:
        new_status = 'pending_ceph'
        logger.info('Upload to Ceph')
    elif status == 3:
        new_status = 'done_ceph'
        logger.info('Upload to LiPAD')
    elif status == 4:
        new_status = 'done'
        logger.info('Metadata uploaded in LiPAD')

    with PSQL_DB.atomic() as txn:
        new_q = (Automation_AutomationJob
                 .update(status=new_status, status_timestamp=datetime.now())
                 .where(Automation_AutomationJob.id == q.id))
        new_q.execute()


def proper_block_name(block_path):

    # input format: ../../Agno_Blk5C_20130418

    # parses blockname from path
    block_name = block_path.split(os.sep)[-1]
    if block_name == '':
        block_name = block_path.split(os.sep)[-2]
    # remove date flown
    block_name = block_name.rsplit('_', 1)[0]

    return block_name


def find_in_coverage(block_name):
    """
        Lidar coverage block in LiPAD DB
        Assume an active connection is present
    """
    try:
        block = Cephgeo_LidarCoverageBlock.get(block_name=block_name)
        uid = block.uid
        print 'Block in Lidar Coverage'
        print 'Block UID:', uid
        return True, uid
    except Exception:
        print 'Block not in Lidar Coverage', block_name
        return False, 0


def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0] + os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0] + os.path.sep


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
        Output directory containing renamed tiles. The final format of a file is:
        **Easting_Northing_FileType_Processor_BlockUID.FileType**

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
                        logger.warning('\nWARNING:  %s', outPath,
                                       'already exists!')
                        ctr += 1
                        # outFN =
                        # ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
                        outFN = 'E{0}N{1}_{2}_{3}_U{4}_{5}.{6}'.format(
                            tile_x, tile_y, typeFile, processor, block_uid,
                            str(ctr), typeFile.lower())
                        # print outFN
                        outPath = os.path.join(outDir, outFN)

                    print 'Path  %s', os.path.join(path, tile), 'Filename: %s', outFN
                    logger.info('Path %s', os.path.join(
                        path, tile), 'Filename: %s', outFN)

                    logger.info('Path + Filename  %s', os.path.join(path, tile) +
                                ' ---------  %s' + outFN + '\n')

                    logger.info(' %s', outPath,
                                'Filename okay. Wont copy data yet')
                    # Copy data
                    shutil.copy(laz_file_path, outPath)
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
