from models import *
from rename_laz_v2 import *
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


def files_renamed(input_dir):
    return False


def ceph_upload(input_dir_ceph):
    print 'Upload to Ceph'
    try:
        output = subprocess.check_output(
            ['./bulk_upload_nonthreaded.py', input_dir_ceph])
        print 'Ceph Output...'
        print output, len(output)
        if 'Done Uploading!' in output:
            print 'Caught Done Uploading!'
        return true

    except Exception:
        print 'Error in Ceph upload!'
        return False


def assign_status(q, status):
    if status == 1:
        q.status = 'in_salad'
    elif status == 2:
        q.status = 'in_ceph'
    q.status_timestamp = datetime.now
    # elif status == 3:


def process_job(q):
    assign_status(q, 1)
    print 'Processing Job'
    datatype = q.datatype
    input_dir = q.input_dir
    # output_dir = q.output_dir
    output_dir = q.output_dir.__add__('/' + block_name)
    processor = q.processor
    date = q.date_submitted
    target_os = q.target_os

    block_name = proper_block_name(input_dir)
    print 'BLOCK NAME', block_name

    in_coverage, block_uid = find_in_coverage(block_name)
    if in_coverage:
        print 'Found in Lidar Coverage model', block_name, block_uid
        if not files_renamed(input_dir):
            rename_laz(input_dir, output_dir, processor, block_uid)
            assign_status(q, 2)
        # else:
        # Should upload directly to Ceph
        ceph_uploaded = ceph_upload(output_dir)
        if ceph_uploaded:
            print 'Upload metadata to LiPAD...'

    else:
        print 'ERROR NOT FOUND IN MODEL', block_name, block_uid


def db_watcher():
    """
        Watch LiPAD DB Automation Table for pending jobs
    """
    print 'Starting...'
    setup_logging()
    connect_db()
    try:
        q = Automation_AutomationJob.get(status='pending')
        process_job(q)
    except Exception as e:
        logger.exception('No pending task')
    close_db()


# setup_logging()
# db_watcher()
