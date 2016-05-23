from pprint import pprint

import logging, traceback
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.tasks import execute
from fabric.context_managers import settings
import os

COMMAND_DICT={  "TILE_DEM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py",
                "TILE_DSM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py -t dsm",
                "TILE_DTM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py -t dtm",
                "UPLOAD"        : "python /home/AD/autotiler/lipad-data-tools/preprocess_data/remote/upload_tiles.py",
                "RENAME_ORTHO"  : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/rename_orthophoto.py",
                "UTM_51N_PRJ"   : "-p /home/AD/autotiler/lipad-data-tools/preprocess_data/remote/WGS_84_UTM_zone_51N.prj",
                "TMP_DIR"       : "--temp-dir /tmp/remote/"
              }
def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0]+os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0]+os.path.sep

#PROJECTION_FILE="/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/WGS_84_UTM_zone_51N.prj"
TILING_REMOTE_HOST="autotiler@palace.dream.upd.edu.ph"
UPLOAD_REMOTE_HOST="autotiler@ceph-radosgw.prd.dream.upd.edu.ph"
METADATA_LOG_DIR=os.path.join(get_cwd(),"dump")

###
###
### TODO: Check Philgrid Data Coverage for duplicate data tiles
###
###

@hosts(TILING_REMOTE_HOST)
def tile_dsm_remote(geostorage_path_to_dsm_dir, geostorage_path_to_output_dir):
    print "Tiling DSM on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_dsm_dir, geostorage_path_to_output_dir)
    src_dir_arg = "-d {0}".format(geostorage_path_to_dsm_dir)
    out_dir_arg = "-o {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} {3} {4} {5}".format( COMMAND_DICT["TILE_DSM"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = None
    with settings(host_string=TILING_REMOTE_HOST):
        result = run(cli_call)
    pprint(result.replace("\\r\\n","\n"))
    if result.failed:
        print "Failed"    


@hosts(TILING_REMOTE_HOST)
def tile_dtm_remote(geostorage_path_to_dtm_dir, geostorage_path_to_output_dir):
    print "Tiling DTM on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_dtm_dir, geostorage_path_to_output_dir)
    src_dir_arg = "-d {0}".format(geostorage_path_to_dtm_dir)
    out_dir_arg = "-o {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} {3} {4} {5}".format( COMMAND_DICT["TILE_DTM"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = None
    with settings(host_string=TILING_REMOTE_HOST):
        result = run(cli_call)
    pprint(result)
    if result.failed:
        print "Failed"    

@hosts(TILING_REMOTE_HOST)
def rename_ortho_remote(geostorage_path_to_ortho_dir, geostorage_path_to_output_dir):
    print "Renaming orthophoto on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_ortho_dir, geostorage_path_to_output_dir)
    src_dir_arg = "-d {0}".format(geostorage_path_to_ortho_dir)
    out_dir_arg = "-op {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} {3} {4} {5}".format( COMMAND_DICT["RENAME_ORTHO"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = None
    with settings(host_string=TILING_REMOTE_HOST):
        result = run(cli_call)
    pprint(result)
    if result.failed:
        print "Failed"    

@hosts(TILING_REMOTE_HOST)
def rename_laz_remote(geostorage_path_to_laz_dir, geostorage_path_to_output_dir):
    print "Renaming LAZ on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_laz_dir, geostorage_path_to_output_dir)
    src_dir_arg = "-i {0}".format(geostorage_path_to_laz_dir)
    out_dir_arg = "-op {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} {3} {4} {5}".format( COMMAND_DICT["RENAME_ORTHO"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = None
    with settings(host_string=TILING_REMOTE_HOST):
        result = run(cli_call)
    pprint(result)
    if result.failed:
        print "Failed"    


@hosts(UPLOAD_REMOTE_HOST)
def upload_tiles(tiled_data_dir, metadata_log_dir):
    print "Uploading tiled data on remote host [{0}] from [{1}]".format(TILING_REMOTE_HOST, tiled_data_dir)
    cli_call =  "{0} {1}".format( COMMAND_DICT["UPLOAD"],
                                         tiled_data_dir,)
    result = None
    with settings(host_string=UPLOAD_REMOTE_HOST):
        result = run(cli_call)
    pprint(result)
    if result.failed:
        print "Failed"    
