from pprint import pprint

import logging, traceback
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.tasks import execute
from django.core.mail import send_mail
import os

COMMAND_DICT={  "TILE_DEM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py",
                "TILE_DSM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py -t dsm",
                "TILE_DTM"      : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/tile_dem.py -t dtm",
                "RENAME_ORTHO"  : "/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/rename_orthophoto.py",
                "UTM_51N_PRJ"   : "-p /home/AD/autotiler/lipad-data-tools/preprocess_data/remote/WGS_84_UTM_zone_51N.prj",
                "TMP_DIR"       : "--temp-dir /tmp/remote/"
              }
#PROJECTION_FILE="/home/AD/autotiler/lipad-data-tools/preprocess_data/remote/WGS_84_UTM_zone_51N.prj"
TILING_REMOTE_HOST="autotiler@palace.dream.upd.edu.ph"

@hosts(TILING_REMOTE_HOST)
def tile_dsm_remote(geostorage_path_to_dsm_dir, geostorage_path_to_output_dir):
    print "Tiling DSM on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_dsm_dir, geostorage_path_to_output_dir)
    src_dir_arg = "-d {0}".format(geostorage_path_to_dsm_dir)
    out_dir_arg = "-o {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} -t dsm {3} {4} {5}".format( COMMAND_DICT["TILE_DEM"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    """
    result = run(COMMAND_DICT["TILE_DEM"],
                     src_dir_arg,
                     out_dir_arg,
                     "-t dsm",
                     COMMAND_DICT["UTM_51N_PRJ"],
                     COMMAND_DICT["TMP_DIR"],
                     log_file_arg)
    """
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
    print "[DEBUG]: {0} {1} {2} {3} {4} {5} -t dtm".format( COMMAND_DICT["TILE_DTM"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = run(COMMAND_DICT["TILE_DTM"],
                     src_dir_arg,
                     out_dir_arg,
                     "-t dtm",
                     COMMAND_DICT["UTM_51N_PRJ"],
                     COMMAND_DICT["TMP_DIR"],
                     log_file_arg)
    pprint(result)
    if result.failed:
        print ""    


