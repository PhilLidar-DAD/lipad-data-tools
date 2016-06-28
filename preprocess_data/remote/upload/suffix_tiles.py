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
    parser.add_argument("-l", "--logfile",dest="logfile",
                        help="Path to resume log file for this upload")
    parser.add_argument("-s", "--suffix",dest="suffix",
                        help="suffix to be appended to the data tile file")
    args = parser.parse_args()
    
    if not (args.suffix):
        parser.error('No action requested, add -suffix or -upload')
    
    suffix = args.suffix

    data_tiles_dir = None
    if isdir(args.dir):
        data_tiles_dir = args.dir
        print("Uploading files from [{0}].".format(args.dir))
    else:
        raise Exception("ERROR: [{0}] is not a valid directory.".format(args.dir))
    
    for path, subdirs, files in walk(self.data_tiles_dir):
        for filename in files:
            name_tokens = filename.split(".")
            if name_tokens[-1] == "tif":
                new_name = os.path.join(name_tokens[:-1], suffix, ".", name_tokens[:-1])
                print "{0} >>> {1}".format(filename, new_name)
                #os.rename(root + os.sep + filename, root + os.sep + new_name)
    
