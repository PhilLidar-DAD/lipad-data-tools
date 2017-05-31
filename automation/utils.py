import os
from models import *
import random
import subprocess
from datetime import datetime
from pprint import pprint
import json
import unicodedata

from models import Cephgeo_LidarCoverageBlock as LidarCoverageBlock
import collections


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

def convert_to_string(data):
    if isinstance(data, basestring):
        #return str(data.encode("ascii"))
        return unicodedata.normalize('NFKD', data).encode('ascii','ignore').strip("\"\'")
    elif isinstance(data, collections.Mapping):
        return dict(map(convert_to_string, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert_to_string, data))
    else:
        return data.strip("\"\'")
    
def parse_dem_input(dem_input):
    parsed_input = unicodedata.normalize('NFKD', dem_input).encode('ascii','ignore').strip("\\'")
    #return json.loads(convert_to_string(parsed_input))
    return json.loads(parsed_input)

def tile_dtm(dem_dir, output_dir):
    """./tile_dem.py -d data/MINDANAO1/v_mdn/ -t dtm -p WGS_84_UTM_zone_51N.prj -o output/"""
    
    dtm_dir = [name for name in os.listdir(dem_dir) if ("cv_" in name or "uv_" in name ) and os.path.isdir(os.path.join(dem_dir,name))][0]
    print 'Tiling DTM in ', dtm_dir
    
    #Check if output directory exists, create it if missing
    tile_output_dir = os.path.join(output_dir,'DTM')
    if not os.path.exists(tile_output_dir):
        os.makedirs(tile_output_dir)
    
    try:
        output = subprocess.check_output(
            ['./tile_dem.py', 
                '-d', dtm_dir,
                '-t','dtm',
                '-p','WGS_84_UTM_zone_51N.prj',
                '-o', tile_output_dir,
                ])
        
        if 'Done Tiling' in output:
            print 'DTM Tiling Done!'
        return True
    except Exception:
        print 'Error in DTM tiling. DTM directory: ' + dtm_dir
        return False

def tile_dsm(dem_dir, output_dir):
    """./tile_dem.py -d data/MINDANAO1/d_mdn/ -t dsm -p WGS_84_UTM_zone_51N.prj -o output/"""
    dsm_dir = os.path.join(dem_dir, [name for name in os.listdir(dem_dir) if ("cd_" in name or "ud_" in name ) and os.path.isdir(os.path.join(dem_dir,name))][0])
    
    for target_dir, dirs, files in os.walk(dsm_dir):
        for name in files:
            if name == "hdr.adf":
                print "TARGET: ", target_dir
                dsm_dir = target_dir
                break
                 
    #Check if output directory exists, create it if missing
    tile_output_dir = os.path.join(output_dir.encode("ascii"),'DSM')
    if not os.path.exists(tile_output_dir):
        os.makedirs(tile_output_dir)
    
    print 'Tiling DSM in [' +dsm_dir+ '] into [' +tile_output_dir+ ']'  
    
    try:
        output = subprocess.check_output(
            ['./tile_dem.py', 
                '-d', dsm_dir,
                '-t','dsm',
                '-p','WGS_84_UTM_zone_51N.prj',
                '-o', tile_output_dir,
                ])
        
        if 'Done Tiling' in output:
            print 'DSM Tiling Done!'
        return True
    except subprocess.CalledProcessError as e:
        print 'Error in DSM tiling. DSM directory: ' + dsm_dir
        print e.output
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
        # get block_name in LidarCoverageBlock Model
        block = LidarCoverageBlock.get(block_name=block_name)
        print 'Block ['+str(block_name)+'] in Lidar Coverage, uid: ', block.uid
        return [True, block.uid, block_name.encode('ascii') ]
    except Exception:
        print 'No Block ['+str(block_name)+'] in Lidar Coverage'
        return [False, 0, block_name.encode('ascii') ]
