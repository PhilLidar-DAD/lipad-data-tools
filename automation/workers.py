from models import *
from rename_tiles import *
from transfer_metadata import *
from utils import *
import os
import sys
import logging
import random
import time
import subprocess
from datetime import datetime

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
            print 'DTM datatype'

        assign_status(q, 2)
        print 'Status', q.status
        print 'Status Timestamp', q.status_timestamp
        ceph_uploaded, log_file = ceph_upload(output_dir)

        if ceph_uploaded:
            assign_status(q, 3)
            transfer_metadata(log_file, datatype)

    else:
        print 'ERROR NOT FOUND IN MODEL', block_name, block_uid


def db_watcher():
    """
        Watch LiPAD DB Automation Table for pending jobs
    """
    print 'Starting...'
    setup_logging()
    while True:
        connect_db()
        status = ['pending_process', 'done_ceph']
        for s in status:
            try:
                q = Automation_AutomationJob.get(status=s)
                if s.__eq__('pending_process'):
                    if q.target_os.lower() == 'linux':
                        process_job(q)
                        # elif q.datatype.lower() == 'dtm':
                    else:
                        print 'PASS TO WINDOWS'
                        # Windows poller
                elif s.__eq__('done_ceph'):
                    # in case upload from ceph to lipad was interrupted
                    assign_status(q, 3)
                    transfer_metadata()

            except Automation_AutomationJob.DoesNotExist:
                logger.error('No %s task', s)

            except Exception:
                logger.exception('Database watcher error!')
            finally:
                close_db()

        delay = get_delay(1, 10)
        logger.info('Worker Sleeping for %ssecs...', delay)
        time.sleep(delay)
