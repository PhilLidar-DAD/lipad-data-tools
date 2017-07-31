from models import *
# from rename_tiles import *
from transfer_metadata import *
from utils import *
from exceptions import *
import os
import sys
import logging
import random
import time
import subprocess
from datetime import datetime
import json as py_json


logger = logging.getLogger()
LOG_LEVEL = logging.DEBUG
CONS_LOG_LEVEL = logging.DEBUG
FILE_LOG_LEVEL = logging.DEBUG


def setup_logging():

    # Setup logging
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d)\t: %(message)s')

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Setup file logging
    LOG_FILE = os.path.splitext(__file__)[0] + '.log'
    fh = logging.FileHandler(LOG_FILE, mode='w')
    fh.setLevel(FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def connect_db():
    retry = True
    while retry:
        try:
            PSQL_DB.connect()
            print 'Connected to DB...'
            retry = False
            print 'Retry', retry
        except Exception:
            delay = random.randint(0, 30)
            logger.exception(
                'Error connecting to database. Retrying in %ss...', delay)
            retry = True
            time.sleep(delay)


def close_db():
    if not PSQL_DB.is_closed():
        PSQL_DB.close()


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
    """
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    datatype = q.datatype
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor

    block_name = proper_block_name(input_dir)
    output_dir = q.output_dir.__add__('/' + block_name)

    print 'BLOCK NAME', block_name

    in_coverage, block_uid = find_in_coverage(block_name)

    # check first if Block Name is in Lidar Coverage
    if in_coverage:
        print 'Found in Lidar Coverage model', block_name, block_uid
        if datatype.lower() == ('laz' or 'ortho'):
            if not files_renamed(input_dir):
                rename_tiles(input_dir, output_dir, processor, block_uid)
        else:
            raise Exception(
                'Handler not implemented for type: ' + str(q.datatype))

        assign_status(q, 2)
        print 'Status', q.status
        print 'Status Timestamp', q.status_timestamp
        ceph_uploaded, log_file = ceph_upload(output_dir)

        if ceph_uploaded:
            assign_status(q, 3)
            transfer_metadata(log_file, datatype)

    else:
        print 'ERROR NOT FOUND IN MODEL', block_name, block_uid


def parse_dem_input(input_str):
    tokens = input_str.strip().replace(' ', '').split(',')
    block_names = tokens[1:]
    dem_dir = os.path.join("/mnt/pmsat_pool/geostorage/DPC/",
                           tokens[0].replace('\\', '/').split('DPC/')[1])

    # if not os.path.isdir(dem_dir):
    if False:
        raise DirectoryNotFoundException(
            "Directory does not exist: " + dem_dir)

    return dem_dir, block_names


def handle_dem(q):
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor

    dem_dir, block_name_list = parse_dem_input(input_dir)

    print 'BLOCKS: ' + str(block_name_list)

    # Convert block_names to block_uids
    block_uid_list = []
    for block_name in block_name_list:
        in_coverage, block_uid = find_in_coverage(block_name)
        block_uid_list.append(tuple(block_uid, in_coverage))

    block_uid_list_json = py_json.dumps(block_uid_list)

    """
        @TODO:
        1) Tile blocks for each metadata
        2) Pass to LiPAD db
        3) Talk to DJ about file and metadata naming conventions
    """

    if q.datatype.lower == 'DTM':
        """Tile DTM only"""
        tile_dtm(dem_dir, output_dir)
    elif q.datatype.lower == 'DSM':
        """Tile DSM only"""
        tile_dsm(dem_dir, output_dir)
    elif q.datatype.lower == 'DEM' or q.datatype.lower == 'DTM/DSM':
        """Tile both DEMS"""

    """
        @TODO:
        1) Upload tiles
        2) Pass CephDataObject metadata to LiPAD db
    """
    assign_status(q, 2)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    ceph_uploaded, log_file = ceph_upload(output_dir)

    if ceph_uploaded:
        assign_status(q, 3)
        transfer_metadata(log_file)


def db_watcher():
    """Watch LiPAD Database AutomationJob for pending jobs.

    Connect the ORM to the database. This loops watching of AutomationJob for
    new workers. This also checks the status of each AutomationJob object. This
    passes the worker to its repective workflow depending on its status.

    This starts upon startup of processing environment.
    """

    print 'Starting...'
    setup_logging()
    while True:
        connect_db()

        for status in Automation_AutomationJob.STATUS_CHOICES:
            try:
                q = Automation_AutomationJob.get(status=status)
                print 'Fetched Query'
                print 'Status:', q.status
                if q.status.__eq__('pending_process'):
                    if q.target_os.lower() == 'linux':
                        process_job(q)
                        # elif q.datatype.lower() == 'dtm':
                    else:
                        print 'PASS TO WINDOWS'
                        # Windows poller
                elif q.status.__eq__('done_ceph'):
                    # in case upload from ceph to lipad was interrupted
                    assign_status(q, 3)
                    # transfer_metadata()
                elif q.status.__eq__('pending_ceph'):
                    pass
                elif q.status.__eq__('done_ceph'):
                    pass
                elif q.status.__eq__('done'):
                    pass

            except Automation_AutomationJob.DoesNotExist:
                logger.error('No %s task', status)

            except Exception:
                logger.exception('Database watcher error!')
            finally:
                close_db()

        delay = get_delay(1, 10)
        logger.info('Worker Sleeping for %ssecs...', delay)
        time.sleep(delay)
