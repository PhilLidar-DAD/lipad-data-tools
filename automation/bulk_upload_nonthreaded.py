#!/usr/bin/env python
from pprint import pprint
from os import listdir, walk
from os.path import isfile, isdir, join
import argparse
import time
import os
from collections import OrderedDict
from ConfigParser import SafeConfigParser
import argparse
import ConfigParser
import os
import sys
import shutil

CONFIG = ConfigParser.ConfigParser()
CONFIG.read(os.path.join(os.path.dirname(
    os.path.realpath(__file__)), "config.ini"))


def build_ceph_dict(config):
    dict1 = {}
    options = config.options("ceph")
    for option in options:
        try:
            dict1[option] = config.get("ceph", option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def get_cwd():
    cur_path = os.path.realpath(__file__)
    if "?" in cur_path:
        return cur_path.rpartition("?")[0].rpartition(os.path.sep)[0] + os.path.sep
    else:
        return cur_path.rpartition(os.path.sep)[0] + os.path.sep


def setup_dump_and_logs():

    directories = ["dump", "logs"]
    cwd = get_cwd()

    for d in directories:
        if not os.path.exists(join(cwd, d)):
            os.makedirs(join(cwd, d))

    logfiles = ["logs/bulk_upload.log", "logs/ceph_storage.log"]

    for f in logfiles:
        if not os.path.isfile(os.path.join(cwd, f)):
            with open(os.path.join(cwd, f), 'wb') as temp_file:
                temp_file.write("")


def write_obj_metadata_to_csv(obj_metadata):
    pass

# config_parser = SafeConfigParser()
# config_parser.read(get_cwd() + 'config.ini')


# Default virtualenv path to activate file
# activate_this_file = "~/.virtualenvs/geonode/bin/activate_this.py"
# activate_this_file ="~/.virtualenvs/automation/bin/activate_this.py"
activate_this_file = CONFIG.get('env', 'activatethis')

# Default log filepath
log_filepath = get_cwd() + "logs/bulk_upload.log"

# Ceph Object Gateway Settings
CEPH_OGW = build_ceph_dict(CONFIG)

# Parse CLI arguments
parser = argparse.ArgumentParser()

parser.add_argument("dir",
                    help="Directory containing the tiled files and named according to their grid reference")
parser.add_argument("-e", "--virtualenv", dest="venv",
                    help="Path to the virtualenv activate_this.py file")
parser.add_argument("-l", "--logfile", dest="logfile",
                    help="Path to log file for this upload")
parser.add_argument("-r", "--resume", dest="resume",
                    help="Resume from a interrupted upload using the CSV dump")

args = parser.parse_args()
# pprint(args)

# Check if --logfile is set
if args.logfile is not None:
    if isfile(args.logfile):
        log_filepath = args.logfile

# Try activating the virtualenv, error out if it cannot be activated
# Check if --virtualenv is set
if args.venv is not None:
    if isfile(args.venv):
        activate_this_file = args.venv
    else:
        raise Exception("ERROR: Failed to activate environment. Cannot find\n \
                            virtualenv activate file in: [{0}]".format(args.venv))
    try:
        execfile(activate_this_file, dict(__file__=activate_this_file))
    except IOError as e:
        print "ERROR: Failed to activate environment. Check if virtualenv\n \
                 activate file is found in [{0}]".format(activate_this_file)
        raise e

# Import after activating virtualenv
from ceph_client import CephStorageClient
import warnings
import mimetypes
import logging

# if __name__ == "__main__":

setup_dump_and_logs()

# Initialize logging
logging.basicConfig(filename=log_filepath, level=logging.DEBUG)
logger = logging.getLogger('bulk_upload_nonthreaded.py')

# Set the log format and log level
logger.setLevel(logging.DEBUG)
# log.setLevel(logging.INFO)

# Set the log format.
stream = logging.StreamHandler()
logformat = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%b %d %H:%M:%S')
stream.setFormatter(logformat)

logger.addHandler(stream)

#grid_files_dir = "/home/geonode/grid_data"
grid_files_dir = None
if isdir(args.dir):
    grid_files_dir = args.dir
    # print("Uploading files from [{0}].".format(args.dir))
    logger.info("Uploading files from [{0}].".format(args.dir))
else:
    raise Exception("ERROR: [{0}] is not a valid directory.".format(args.dir))

original_filters = warnings.filters[:]

# Ignore warnings.
warnings.simplefilter("ignore")

########################
###  MAIN LOOP CALL  ###
########################

uploaded_objects = []
ceph_client = CephStorageClient(CEPH_OGW['user'],
                                CEPH_OGW['key'],
                                CEPH_OGW['url'],
                                container_name=CEPH_OGW['container'])
# Connect to Ceph Storage
ceph_client.connect()
logger.info("Connected to Ceph OGW at URI [{0}]".format(CEPH_OGW['url']))

# List of allowed file extensions
allowed_files_exts = ["tif", "laz"]
logger.info("Script will now upload files with the extensions {0}".format(
    allowed_files_exts))
logger.info("=====================================================================".format(
    allowed_files_exts))

top_dir_name = filter(None, grid_files_dir.split(os.path.sep))[-1]
data_dump_file_path = "dump/uploaded_objects_[{0}]_{1}.txt".format(
    top_dir_name, time.strftime("%Y-%m-%d-%H%M-%S"))

with open(data_dump_file_path, 'w') as dump_file:
    header_str = "NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH GRID_REF\n"
    print('NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH GRID_REF\n')
    dump_file.write(header_str)

    # No previous metadata dump file to resume from specified
    if args.resume is None:
        for path, subdirs, files in walk(grid_files_dir):
            for name in files:
                # Upload each file
                filename_tokens = name.rsplit(".")

                # Check if file is in allowed file extensions list
                if filename_tokens[-1] in allowed_files_exts:
                    grid_ref = filename_tokens[0].rsplit("_")[0]
                    file_path = join(path, name)

                    #upload_file(file_path, grid_ref)
                    obj_dict = ceph_client.upload_file_from_path(file_path)
                    obj_dict['grid_ref'] = grid_ref
                    uploaded_objects.append(obj_dict)
                    logger.info("Uploaded file [{0}]".format(join(path, name)))

                    ### TODO ###
                    dump_file.write("{0},{1},{2},{3},{4},{5}\n".
                                    format(obj_dict['name'],                                                   obj_dict[
                                        'last_modified'],
                                        obj_dict[
                                        'bytes'],
                                        obj_dict[
                                        'content_type'],
                                        obj_dict[
                                        'hash'],
                                        obj_dict['grid_ref']))

                    print("{0},{1},{2},{3},{4},{5}".
                          format(obj_dict['name'],                                                   obj_dict[
                              'last_modified'],
                              obj_dict[
                              'bytes'],
                              obj_dict[
                              'content_type'],
                              obj_dict[
                              'hash'],
                              obj_dict['grid_ref']))

                else:
                    logger.debug(
                        "Skipped file [{0}]. Not allowed in file extensions".format(join(path, name)))

    else:
        csv_delimiter = ','
        csv_columns = 6
        # Use ordered dictionary to remember roder fo insertion
        uploaded_csv_dict = OrderedDict()
        prev_data_dump_file_path = args.resume
        new_dump_file = data_dump_file_path
        filenames_list = []

        logger.info("Resuming previous upload from dump file[{0}]".format(
            prev_data_dump_file_path))
        # Check and read specified dump file to resume to
        if not os.path.isfile(prev_data_dump_file_path):
            raise Exception("Dump file [{0}] is not a valid file!".format(
                prev_data_dump_file_path))

        # Index all previously uploaded files into list, using the filename as
        # key
        with open(prev_data_dump_file_path, "r") as prev_dump_file:
            for csv_line in prev_dump_file:
                if not csv_line == header_str:
                    metadata_list = csv_line.split(csv_delimiter)
                    uploaded_csv_dict[metadata_list[0]] = csv_line

        # Assume last entry as malformed, so remove it (could be the EOF)
        uploaded_csv_dict.popitem(last=True)
        logger.info("Loaded [{0}] objects from dump file".format(
            len(uploaded_csv_dict)))

        # Write previous metadata into new dump file
        logger.info(
            "Writing previously uploaded object metadata into new dump file...")
        dump_file.writelines(uploaded_csv_dict.values())
        logger.info("Wrote [{0}] object metadata CSVs into new dump file...".format(
            len(uploaded_csv_dict)))

        # Upload the objects, skipping those in the list of previously uploaded
        filenames_list = uploaded_csv_dict.keys()
        for path, subdirs, files in walk(grid_files_dir):
            for name in files:

                # Check if file has already been uploaded
                if name not in filenames_list:
                    # Upload each file
                    filename_tokens = name.rsplit(".")

                    # Check if file is in allowed file extensions list
                    if filename_tokens[-1] in allowed_files_exts:
                        grid_ref = filename_tokens[0].rsplit("_")[0]
                        file_path = join(path, name)

                        #upload_file(file_path, grid_ref)
                        obj_dict = ceph_client.upload_file_from_path(file_path)
                        obj_dict['grid_ref'] = grid_ref
                        uploaded_objects.append(obj_dict)
                        logger.info(
                            "Uploaded file [{0}]".format(join(path, name)))

                        ### TODO ###
                        # write metadata for file into dumpfile in CSV format
                        metadata_csv = "{0},{1},{2},{3},{4},{5}\n".format(obj_dict['name'],
                                                                          obj_dict[
                            'last_modified'],
                            obj_dict[
                            'bytes'],
                            obj_dict[
                            'content_type'],
                            obj_dict[
                            'hash'],
                            obj_dict['grid_ref'])
                        # Skip if previously uploaded
                        dump_file.write(metadata_csv)

                        print(metadata_csv)

                    else:
                        logger.debug(
                            "Skipped unallowed file [{0}]".format(join(path, name)))
                else:
                    logger.debug(
                        "Skipped previously uploaded file [{0}]".format(join(path, name)))

    dump_file.write("---END---\n")

# Close Ceph Connection
ceph_client.close_connection()

print("Done Uploading!")
# pprint(uploaded_objects)
# print("wrote metadata to file:")
# print("{0}".format(data_dump_file_path))

# print 'File Path: ', data_dump_file_path
# return data_dump_file_path
