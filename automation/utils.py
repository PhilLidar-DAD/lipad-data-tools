import os
from models import PSQL_DB, Automation_AutomationJob, Cephgeo_LidarCoverageBlock
import random
import subprocess
from datetime import datetime
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


def ceph_upload(input_dir):
    # @TODO
    # Separate logging field in Automation Model
    stream = setup_logging()

    print 'Uploading to Ceph ....'
    logger.info('Uploading to Ceph ....')

    # PATH = './bulk_upload_nonthreaded.py'

    try:
        # if not os.path.isfile(PATH):
        #     print "File does not exist", PATH
        # if not os.access(PATH, os.R_OK):
        #     print "File is not readable", PATH
        # return True, ''

        output = subprocess.check_output(['./bulk_upload_nonthreaded.py', input_dir])

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
            logger.info('Done Uploading!')
            logger.info('Filename %s', filename)
        return True, filename

    except Exception:
        print 'Error in Ceph upload!'
        logger.exception()
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


def assign_status(q):
    status = q.status
    print 'Status:', status
    logger.info('Status: %s', status)

    for status_index in [status_index for status_index, x in enumerate(Automation_AutomationJob.STATUS_CHOICES)
                         if x == status]:
        status_index += 1

    new_status = Automation_AutomationJob.STATUS_CHOICES[status_index]

    if status_index == 1:
        logger.info('Processed Job')
    elif status_index == 2:
        logger.info('Upload to Ceph')
    elif status_index == 3:
        logger.info('Upload to LiPAD')
    elif status_index == 4:
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
