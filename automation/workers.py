from models import *
from rename_tiles import *
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
from pprint import pprint
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




def handle_dem(q):
    assign_status(q, 1)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    print 'Processing Job'
    input_dir = q.input_dir.strip("\'\"")
    output_dir = q.output_dir.strip("\'\"")
    processor = q.processor
    
    dem_dict = parse_dem_input(input_dir)
    dem_file_path = dem_dict["dem_file_path"].strip("\'\"")
    block_list =  dem_dict["blocks"]

    print 'BLOCKS FOUND: ', len(block_list)

    # Convert block_names to block_uids
    block_uid_list = []
    for block in block_list:
        block_name = block[0]
        block_uid_list.append(find_in_coverage(block_name))
        
    print "DEBUG: BLOCK UIDs"
    pprint(block_uid_list)

    block_uid_list_json = py_json.dumps(block_uid_list)

    if q.datatype.lower() == 'dtm':
        """Tile DTM only"""
        tile_dtm(dem_file_path, output_dir)
    elif q.datatype.lower() == 'dsm':
        """Tile DSM only"""
        tile_dsm(dem_file_path, output_dir)
    elif q.datatype.lower() == 'dem' or q.datatype.lower == 'dtm/dsm':
        """Tile both DEMS"""
        print "dtm/dsm dual tiling not yet implemented"
    else:
        print "Nothing to do with dataype of ["+str(q.datatype)+"]"
    
    """
        @TODO:
        1) Tile blocks for each metadata
        2) Pass to LiPAD db
        3) Upload Lidar Coverage to test VM -DONE
    """

    assign_status(q, 2)
    print 'Status', q.status
    print 'Status Timestamp', q.status_timestamp
    ceph_uploaded, log_file = ceph_upload(output_dir)

    #DEBUG    
    return

    if ceph_uploaded:
        assign_status(q, 3)
        transfer_metadata(log_file)
    """
        @TODO:
        1) Upload tiles - DONE
        2) Pass CephDataObject metadata to LiPAD db
    """
    

def db_watcher():
    """
        Watch LiPAD DB Automation Table for pending jobs
    """
    print 'Starting...'
    setup_logging()
    while True:
        connect_db()

        for status in Automation_AutomationJob.STATUS_CHOICES:
            try:
                q = Automation_AutomationJob.get(status=status)
                print 'Query found!'
                print q.status
                # if s.__eq__('pending_process'):
                #     if q.target_os.lower() == 'linux':
                #         process_job(q)
                #         # elif q.datatype.lower() == 'dtm':
                #     else:
                #         print 'PASS TO WINDOWS'
                #         # Windows poller
                # elif s.__eq__('done_ceph'):
                #     # in case upload from ceph to lipad was interrupted
                #     assign_status(q, 3)
                #     transfer_metadata()

            except Automation_AutomationJob.DoesNotExist:
                logger.error('No %s task', status)

            except Exception:
                logger.exception('Database watcher error!')
            finally:
                close_db()

        delay = get_delay(1, 10)
        logger.info('Worker Sleeping for %ssecs...', delay)
        time.sleep(delay)
