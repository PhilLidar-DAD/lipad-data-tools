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

_logger = logging.getLogger(__name__)
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG

HEADER_LINE = "NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH,GRID_REF"
FOOTER_LINE = "===END==="
CSV_DELIMITER = ","

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

def parse_ceph_config(config):
    ceph_ogw = dict()
    options = config.options("ceph")
    for option in options:
        try:
            ceph_ogw[option] = config.get("ceph", option)
            if ceph_ogw[option] == -1:
                _logger.warn("skip: %s" % option)
        except:
            _logger.error("exception on %s!" % option)
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

def build_resume_dict(tiles_resume_csv):
    csv_delimiter =','
    csv_columns   = 6
    resume_dict=OrderedDict()     # Use ordered dictionary to remember order of insertion
    filenames_list=[]
    
    # Check and read specified dump file to resume to
    if not os.path.isfile(tiles_resume_csv):
        _logger.error("Metadata log file [{0}] is not a valid file!".format(tiles_resume_csv))
        sys.exit(1)
    else:
        _logger.info("Resuming previous upload from metadata log file[{0}]".format(tiles_resume_csv))
    
    # Index all previously uploaded files into list, using the filename as key
    with open(tiles_resume_csv, "r") as resume_log:
        for csv_line in resume_log:
            if not csv_line == HEADER_LINE:
                metadata_list = csv_line.split(csv_delimiter) # Write each line to new metadata log as well
                resume_dict[metadata_list[0]]=csv_line.rstrip()
                
    _logger.info("Loaded [{0}] objects from previous metadata log file".format(len(resume_dict)))
    
    return resume_dict
    
def setup_metadata_log(meta_log_file_path):
    # Set up metadata logger
    meta_logger = logging.getLogger('MetadataLogger')
    meta_logger.setLevel(logging.DEBUG)

    if "." in meta_log_file_path:
        path_tokens = meta_log_file_path.split('.')
        meta_log_file_path = "{0}_{1}.{2}.inc".format(path_tokens[0], time.strftime("%Y-%m-%d-%H%M-%S"), path_tokens[1])
    else:
        meta_log_file_path = "{0}_{1}.csv.inc".format(meta_log_file_path, time.strftime("%Y-%m-%d-%H%M-%S"))
    
    # Add the log message handler to the logger
    log_file_handler = logging.FileHandler(meta_log_file_path)
    log_file_handler.setFormatter(logging.Formatter('%(message)s'))
    log_file_handler.setLevel(logging.INFO)
    meta_logger.addHandler(log_file_handler)
    
    return meta_logger, meta_log_file_path

def upload_tiles(tiles_dir, csv_out_file, resume_csv=None):
    #Init Ceph OGW settings from config.ini
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini")) 
    
    
    ceph_ogw_dict = parse_ceph_config(config)
    ceph_conn = ceph_connect(ceph_ogw_dict)
    
    print csv_out_file
    meta_logger, meta_log_file_path = setup_metadata_log(csv_out_file)
    
    resume_dict = None
    if resume_csv:
        resume_dict = build_resume_dict(resume_csv)
    
    allowed_files_exts = config.get("file_types", "allowed").replace(' ', '').split(',')
    _logger.info("Script will now upload files with the extensions {0}".format(allowed_files_exts))
    _logger.info("=====================================================================")
    # Begin uploading data tiles
    meta_logger.info(HEADER_LINE)   # Write CSV header
    for path, subdirs, files in walk(tiles_dir):
        for name in files:
            if (resume_dict is not None and name in resume_dict):
                _logger.info("Skipping previously uploaded file [{0}]".format(join(path, name)))
                meta_logger.info(resume_dict[name])
            else:
                # Upload each file
                filename_tokens = name.rsplit(".")
                # Check if file is in allowed file extensions list 
                if filename_tokens[-1] in allowed_files_exts:
                    grid_ref = filename_tokens[0].rsplit("_")[0]
                    file_path = join(path, name)
                    
                    try:
                        # Upload_file(file_path, grid_ref)
                        obj_dict = ceph_conn.upload_file_from_path(file_path)
                        obj_dict['grid_ref'] = grid_ref
                        #uploaded_objects.append(obj_dict)
                        print "Uploaded file [{0}]".format(join(path, name))
                        
                        # Write metadata for file into dumpfile in CSV format
                        metadata_csv="{0},{1},{2},{3},{4},{5}".format(obj_dict['name'],
                                                                        obj_dict['last_modified'],
                                                                        obj_dict['bytes'],
                                                                        obj_dict['content_type'],
                                                                        obj_dict['hash'],
                                                                        obj_dict['grid_ref'])
                        meta_logger.info(metadata_csv)
                    except Exception as e:
                        print e
                else:
                    _logger.info("Skipped unallowed file [{0}]".format(join(path, name)))
    
    meta_logger.info(FOOTER_LINE)   # Write CSV footer
    os.rename(meta_log_file_path, meta_log_file_path.replace(".inc",""))    # Remove .inc in completed logger
    _logger.info("Done, CSV dump found at [{0}]".format(meta_log_file_path))
    
    
if __name__ == "__main__": 
    
    # CLI Arguments
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", 
                        help="Directory containing the tiled files and named according to their grid reference")
    parser.add_argument("-c", "--csv",dest="csv",required=True,
                        help="CSV file path to write Ceph Object meta info")
    parser.add_argument("-r", "--resume",dest="resume",
                        help="Resume from a interrupted upload using the CSV dump")
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-l", "--logfile",
                        help="Filename of logfile")
    
    args = parser.parse_args()
    
    _setup_logging(args)
    
    upload_tiles(args.dir, args.csv, args.resume)