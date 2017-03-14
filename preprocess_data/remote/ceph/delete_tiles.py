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

# Utility Functions

def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0]+os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0]+os.path.sep

if __name__ == "__main__": 
    
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", 
                        help="Directory containing the tiled files and named according to their grid reference")
    parser.add_argument("-r", "--resume",dest="resume",
                        help="Resume from a interrupted upload using the CSV dump")
    args = parser.parse_args()

    data_tiles_dir = None
    if isdir(args.dir):
        data_tiles_dir = args.dir
        print("Uploading files from [{0}].".format(args.dir))
    else:
        raise Exception("ERROR: [{0}] is not a valid directory.".format(args.dir))

    bupload = BulkUpload(args.dir)
    # No resume log specified
    if args.resume is not None:
        bupload.build_resume_dict(args.resume)
    
    bupload.upload_data_tiles()
