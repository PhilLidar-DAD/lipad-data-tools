import os
from models import *
import random
import subprocess
from datetime import datetime
import json


def ceph_upload(input_dir_ceph):
    print 'Upload to Ceph'
    try:
        output = subprocess.check_output(
            ['./bulk_upload_nonthreaded.py', input_dir_ceph])
        print 'Ceph Output...'
        print output, len(output)
        filename = output.split('\n')[-1]
        print 'Logfile', filename
        if 'Done Uploading!' in output:
            print 'Caught Done Uploading!'
        return True, filename
    except Exception:
        print 'Error in Ceph upload!'
        return False, None

def parse_dem_input(dem_input):
    return json.loads(dem_input)

def tile_dtm(dem_dir, output_dir):
    """./tile_dem.py -d data/MINDANAO1/v_mdn/ -t dtm -p WGS_84_UTM_zone_51N.prj -o output/"""
    
    print 'Tiling DTM'
    
    dtm_dir = [name for name in os.listdir(dem_dir) if ("cv_" in name or "uv_" in name ) and os.path.isdir(os.path.join(dem_dir,name))][0]
    
    try:
        output = subprocess.check_output(
            ['./tile_dem.py', 
                '-d', dtm_dir,
                '-t','dtm',
                '-p','WGS_84_UTM_zone_51N.prj',
                '-o', os.path.join(output_dir,'DTM'),
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
    
    dsm_dir = [name for name in os.listdir(dem_dir) if ("cd_" in name or "ud_" in name ) and os.path.isdir(os.path.join(dem_dir,name))][0]
    
    try:
        output = subprocess.check_output(
            ['./tile_dem.py', 
                '-d', dsm_dir,
                '-t','dsm',
                '-p','WGS_84_UTM_zone_51N.prj',
                '-o', os.path.join(output_dir,'DSM'),
                ])
        
        if 'Done Tiling' in output:
            print 'DSM Tiling Done!'
        return True
    except Exception:
        print 'Error in DSM tiling. DSM directory: ' + dsm_dir
        return False, None

def files_renamed(input_dir):
    return False


def get_delay(min_, max_):
    return float('%.2f' % random.uniform(min_, max_))


def assign_status(q, status):
    if status == 1:
        new_status = 'done_process'
    elif status == 2:
        new_status = 'pending_ceph'
    elif status == 3:
        new_status = 'done_ceph'

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
