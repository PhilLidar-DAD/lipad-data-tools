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
    return CephStorageClient(   ceph_ogw_dict['user'],
                                ceph_ogw_dict['key'],
                                ceph_ogw_dict['url'],
                                container_name=ceph_ogw_dict['container'])

def delete_tiles(csv_file_path):
    ceph_ogw_dict = parse_ceph_config()
    ceph_conn = ceph_connect(ceph_ogw_dict)
    
    # Parse csv and delete files
    header_line = "NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH,GRID_REF"
    footer_line = "===END==="
    csv_delimiter = ","
    with open(csv_file_path, "r") as tiles_csv_fh:
        for csv_line in tiles_csv_fh:
            if not csv_line == header_line:
                metadata_list = csv_line.split(csv_delimiter) # Write each line to new metadata log as well
                object_name = metadata_list[0]
                ceph_conn.delete_object(object_name, container_name=ceph_ogw_dict['container'])

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("\nPlease respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
                
if __name__ == "__main__": 
    
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", 
                        help="CSV file containing files to be deleted    ")
    parser.add_argument("-f", "--force",dest="force",
                        help="Skip confirmation fo deletion")
    args = parser.parse_args()

    if not args.force:
        if not query_yes_no("Are you sure to delete all objects listed in:\n[{0}]?\n".format(args.csv)):
            print "\nExiting script..."
            sys.exit()
    
    print "Deleting listed files/objects..."
    delete_tiles(args.csv)