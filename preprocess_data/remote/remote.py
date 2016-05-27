from pprint import pprint

import logging, traceback
from fabric.api import *
from fabric.contrib.console import confirm
from fabric.tasks import execute
from fabric.context_managers import settings
from datetime import datetime
import argparse, ConfigParser, os, sys, shutil, logging

class RemoteScriptFailedException(Exception):
    pass

def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0]+os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0]+os.path.sep

config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.path.join(get_cwd(),"remote.conf")))

COMMAND_DICT={  "TILE_DSM"      : config.get('scripts', 'tile_dsm'),
                "TILE_DTM"      : config.get('scripts', 'tile_dtm'),
                "UPLOAD"        : config.get('scripts', 'upload'),
                "RENAME_ORTHO"  : config.get('scripts', 'rename_ortho'),
                "RENAME_LAZ"    : config.get('scripts', 'rename_laz'),
                "UTM_51N_PRJ"   : config.get('args', 'utm_51n_prj'),
                "TMP_DIR"       : config.get('args', 'tmp_dir'),
                "LOCAL_LOG_DIR" : config.get('logs', 'dir'),
              }

if not os.path.isdir(COMMAND_DICT["LOCAL_LOG_DIR"]):
    os.makedirs(COMMAND_DICT["LOCAL_LOG_DIR"])

TILING_REMOTE_HOST=config.get('hosts', 'tiling')
UPLOAD_REMOTE_HOST=config.get('hosts', 'upload')
# METADATA_LOG_DIR=os.path.join(get_cwd(),"dump")

"""
    TODO: Check Philgrid Data Coverage for duplicate data tiles
"""

@hosts(TILING_REMOTE_HOST)
def tile_dsm(geostorage_path_to_dsm_dir, geostorage_path_to_output_dir):
    print "Tiling DSM on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_dsm_dir, geostorage_path_to_output_dir)
    
    top_dir = os.path.dirname(geostorage_path_to_output_dir)
    base_dir_name = os.path.basename(geostorage_path_to_output_dir)
    top_dir_test = run("[ -d {0} ]".format(top_dir))
    if top_dir_test.return_code == 1:
        raise RemoteScriptFailedException("Cannot create output dir named [{0}]. Cannot find directory [{1}]".format(base_dir_name, top_dir))
    
    #Create remote output directory
    cli_call = "mkdir {0}".format(geostorage_path_to_output_dir)
    result = run(cli_call)
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

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
    pprint(result.replace(r"\r\n", r"\n"))
    
    # Retrieve remote log to local
    get(remote_path=os.path.join(geostorage_path_to_output_dir, "remote.log"), 
        local_path=os.path.join(COMMAND_DICT["LOCAL_LOG_DIR"], "tile_dsm-{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    if result.failed:
        # Copy log file to local
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))


@hosts(TILING_REMOTE_HOST)
def tile_dtm(geostorage_path_to_dtm_dir, geostorage_path_to_output_dir):
    print "Tiling DTM on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_dtm_dir, geostorage_path_to_output_dir)
    
    top_dir = os.path.dirname(geostorage_path_to_output_dir)
    base_dir_name = os.path.basename(geostorage_path_to_output_dir)
    top_dir_test = run("[ -d {0} ]".format(top_dir))
    if top_dir_test.return_code == 1:
        raise RemoteScriptFailedException("Cannot create output dir named [{0}]. Cannot find directory [{1}]".format(base_dir_name, top_dir))
    
    #Create remote output directory
    cli_call = "mkdir {0}".format(geostorage_path_to_output_dir)
    result = run(cli_call)
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

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
    pprint(result.replace(r"\r\n", r"\n"))
    
    # Retrieve remote log to local
    get(remote_path=os.path.join(geostorage_path_to_output_dir, "remote.log"), 
        local_path=os.path.join(COMMAND_DICT["LOCAL_LOG_DIR"], "tile_dtm-{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

@hosts(TILING_REMOTE_HOST)
def rename_ortho(geostorage_path_to_ortho_dir, geostorage_path_to_output_dir):
    print "Renaming orthophoto on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_ortho_dir, geostorage_path_to_output_dir)
    
    top_dir = os.path.dirname(geostorage_path_to_output_dir)
    base_dir_name = os.path.basename(geostorage_path_to_output_dir)
    top_dir_test = run("[ -d {0} ]".format(top_dir))
    if top_dir_test.return_code == 1:
        raise RemoteScriptFailedException("Cannot create output dir named [{0}]. Cannot find directory [{1}]".format(base_dir_name, top_dir))
    
    #Create remote output directory
    cli_call = "mkdir {0}".format(geostorage_path_to_output_dir)
    result = run(cli_call)
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

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
    pprint(result.replace(r"\r\n", r"\n"))
    
    # Retrieve remote log to local
    get(remote_path=os.path.join(geostorage_path_to_output_dir, "remote.log"), 
        local_path=os.path.join(COMMAND_DICT["LOCAL_LOG_DIR"], "rename_ortho-{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

@hosts(TILING_REMOTE_HOST)
def rename_laz(geostorage_path_to_laz_dir, geostorage_path_to_output_dir):
    print "Renaming LAZ on remote host [{0}] from [{1}] and output into [{2}]".format(TILING_REMOTE_HOST, geostorage_path_to_laz_dir, geostorage_path_to_output_dir)
    
    top_dir = os.path.dirname(geostorage_path_to_output_dir)
    base_dir_name = os.path.basename(geostorage_path_to_output_dir)
    top_dir_test = run("[ -d {0} ]".format(top_dir))
    if top_dir_test.return_code == 1:
        raise RemoteScriptFailedException("Cannot create output dir named [{0}]. Cannot find directory [{1}]".format(base_dir_name, top_dir))

    #Create remote output directory
    cli_call = "mkdir {0}".format(geostorage_path_to_output_dir)
    result = run(cli_call)
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

    src_dir_arg = "-i {0}".format(geostorage_path_to_laz_dir)
    out_dir_arg = "-o {0}".format(geostorage_path_to_output_dir)
    log_file_arg = "-l {0}".format(os.path.join(geostorage_path_to_output_dir, "remote.log"))
    cli_call =  "{0} {1} {2} {3} {4} {5}".format( COMMAND_DICT["RENAME_LAZ"],
                                         src_dir_arg,
                                         out_dir_arg,
                                         #COMMAND_DICT["UTM_51N_PRJ"],
                                         COMMAND_DICT["TMP_DIR"],
                                         log_file_arg)
    result = None
    with settings(host_string=TILING_REMOTE_HOST):
        result = run(cli_call)
    pprint(result.replace(r"\r\n", r"\n"))
    
    # Retrieve remote log to local
    get(remote_path=os.path.join(geostorage_path_to_output_dir, "remote.log"), 
        local_path=os.path.join(COMMAND_DICT["LOCAL_LOG_DIR"], "rename_laz-{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S"))))
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))

@hosts(UPLOAD_REMOTE_HOST)
def upload_tiles(tiled_data_dir, metadata_log_dir):
    print "Uploading tiled data on remote host [{0}] from [{1}]".format(TILING_REMOTE_HOST, tiled_data_dir)
    
    dir_test = run("[ -d {0} ]".format(tiled_data_dir))
    if dir_test.return_code == 1:
        raise RemoteScriptFailedException("Cannot7 find tiled data dir: {0}".format(tiled_data_dir))
    
    cli_call =  "{0} {1}".format( COMMAND_DICT["UPLOAD"],
                                         tiled_data_dir,)
    result = None
    with settings(host_string=UPLOAD_REMOTE_HOST):
        result = run(cli_call)
    pprint(result.replace(r"\r\n", r"\n"))
    
    """
    TODO:
        *copy dump to local tmp folder using fabric get
        *read file and parse metadata
        *load each metadata to LiPAD db
    """
    if result.failed:
        raise RemoteScriptFailedException("Failed to run: {0}".format(cli_call))
