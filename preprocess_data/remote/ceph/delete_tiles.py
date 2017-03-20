#!/usr/bin/python
from pprint import pprint
from os import listdir, walk
from os.path import isfile, isdir, join
import argparse, time, os
from collections import OrderedDict
from ConfigParser import SafeConfigParser
from datetime import datetime
import argparse, ConfigParser, os, sys, shutil, logging
from ceph_client import CephStorageClient
import swiftclient
from utils import query_yes_no

_logger = logging.getLogger(__name__)
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG

def _setup_logging(args):
    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter("[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d) : %(message)s")

    # Check verbosity for console
    if args.verbose and args.verbose >= 1:
        global _CONS_LOG_LEVEL
        _CONS_LOG_LEVEL = logging.DEBUG
        
    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Setup file logging
    if args.logfile is not None:
        fh = logging.FileHandler(args.logfile)
        fh.setLevel(_FILE_LOG_LEVEL)
        fh.setFormatter(formatter)
        _logger.addHandler(fh)

def parse_ceph_config():
    #Init Ceph OGW settings from config.ini
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini")) 
    
    ceph_ogw = dict()
    options = config.options("ceph")
    for option in options:
        try:
            ceph_ogw[option] = config.get("ceph", option)
            if ceph_ogw[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            ceph_ogw[option] = None
    
    return ceph_ogw

def ceph_connect(ceph_ogw_dict):
    #Return a Ceph Client Connection
    ceph_conn = CephStorageClient(   ceph_ogw_dict['user'],
                                ceph_ogw_dict['key'],
                                ceph_ogw_dict['url'],
                                container_name=ceph_ogw_dict['container'])
    ceph_conn.connect()
    return ceph_conn

def delete_tiles(csv_file_path):
    ceph_ogw_dict = parse_ceph_config()
    ceph_conn = ceph_connect(ceph_ogw_dict)
    
    # Parse csv and delete files
    header_line = "NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH,GRID_REF"
    footer_line = "===END==="
    csv_delimiter = ","
    with open(csv_file_path, "r") as tiles_csv_fh:
        for csv_line in tiles_csv_fh:
            csv_line = csv_line.strip()
            if not csv_line == header_line and not csv_line == footer_line:
                metadata_tokens = csv_line.split(csv_delimiter) # Write each line to new metadata log as well
                object_name = metadata_tokens[0]
                try:
                    ceph_conn.delete_object(object_name, container=ceph_ogw_dict['container'])
                except swiftclient.exceptions.ClientException as e:
                    if "404 Not Found" in str(e):
                        _logger.info("Skipping [{0}], not found.".format(metadata_tokens[0]))
                    else:
                        raise e

                
if __name__ == "__main__": 
    
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", 
                        help="CSV file containing files to be deleted    ")
    parser.add_argument("-f", "--force",dest="force",
                        help="Skip confirmation fo deletion")
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-l", "--logfile",
                        help="Filename of logfile")
    
    args = parser.parse_args()
    
    _setup_logging(args)
    

    if not args.force:
        if not query_yes_no("Are you sure to delete all objects listed in:\n[{0}]?\n".format(args.csv)):
            _logger.info( "\nExiting script...")
            sys.exit()
    
    _logger.info("Deleting listed files/objects...")
    delete_tiles(args.csv)
