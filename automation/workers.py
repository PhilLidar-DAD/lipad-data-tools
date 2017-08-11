#!/usr/bin/env python2

from models import *

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
from os.path import dirname, abspath

from data_processing import process_job

logger = logging.getLogger()
LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.INFO


def setup_logging():

    # Setup logging
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d)\t: %(message)s')

    # Setup file logging
    filename = __file__.split('/')[-1]
    LOG_FILE_NAME = os.path.splitext(filename)[0] + '.log'
    LOG_FOLDER = dirname(abspath(__file__)) + '/logs/'

    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)

    LOG_FILE = LOG_FOLDER + LOG_FILE_NAME
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
            logger.info('Connected to DB...')
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
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    ceph_uploaded, log_file = ceph_upload(output_dir)

    if ceph_uploaded:
        transfer_metadata(log_file)





def upload_to_ceph(job):
    logger.info('Upload data set to Ceph...')

    is_uploaded, logfile = ceph_upload(job)
    print 'IS uploaded', is_uploaded
    print 'Logfile', logfile

    if is_uploaded:
        print 'STATUS', job.status
        assign_status(job, True)

    # elif not is_uploaded:
    # retry uploading or stop?


def upload_metadata(job):
    uploaded_objects_list = transform_log_to_list(job.ceph_upload_log)

    gridref_dict_by_data_class = ceph_metadata_update(uploaded_objects_list)
    logger.info('Created/Updated Ceph Objects in Database.')



def db_watcher():
    """Watch LiPAD Database AutomationJob for pending jobs.

    Connect ORM to the database. This loops watching of AutomationJob for
    new workers. This also checks the status of each AutomationJob object. This
    passes the worker to its repective workflow depending on its status.

    This starts upon startup of processing environment.

    An `AutomationJob` status is checked and updated.
    """

    print 'Starting...'
    setup_logging()
    while True:
        connect_db()
        for status in Automation_AutomationJob.STATUS_CHOICES:
            try:
                q = Automation_AutomationJob.get(status=status)
                print 'Fetched Query. Status: .', q.status

                #: Pending Job
                if q.status.__eq__('pending_process'):
                    if q.target_os.lower() == 'linux':
                        logger.info('Process in Linux')
                        print 'Process in Linux'
                        process_job(q)
                    else:
                        logger.info('Process in Windows')
                        # @TODO
                        # Windows poller

                #: Processed Job
                elif q.status.__eq__('done_process'):
                    upload_to_ceph(q)

            except Automation_AutomationJob.DoesNotExist:
                logger.info('No %s task', status)

            except Exception:
                logger.exception('Database watcher error!')
            finally:
                close_db()

        delay = get_delay(1, 10)
        logger.info('Worker Sleeping for %ssecs...', delay)
        time.sleep(delay)

db_watcher()
