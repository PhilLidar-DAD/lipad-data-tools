from models import *
from rename_tiles import *
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
        return True

    except Exception:
        print 'Error in Ceph upload!'
        return False


def transfer_metadata():
    print 'Uploading metadata to LiPAD...'


def laz_ortho_worker(q):
    print 'Woker: laz, ortho'
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    input_dir = q.input_dir
    output_dir = q.output_dir
    processor = q.processor

    block_name = proper_block_name(input_dir)
    output_dir = q.output_dir.__add__('/' + block_name)

    print 'BLOCK NAME', block_name

    in_coverage, block_uid = find_in_coverage(block_name)
    if in_coverage:
        print 'Found in Lidar Coverage model', block_name, block_uid
        if not files_renamed(input_dir):
            rename_tiles(input_dir, output_dir, processor, block_uid)
            assign_status(q, 2)
            print 'Status', q.status
            print 'Status Timestamp', q.status_timestamp
        # else:
        # Should upload directly to Ceph
        ceph_uploaded = ceph_upload(output_dir)
        if ceph_uploaded:
            assign_status(q, 3)
            transfer_metadata()

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
                        if q.datatype.lower() == ('laz' | 'ortho'):
                            print 'Pass to worker'
                            laz_ortho_worker(q)
                        # elif q.datatype.lower() == 'dtm':
                    else:
                        print 'PASS TO WINDOWS'
                        # Windows poller
                elif s.__eq__('done_ceph'):
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
