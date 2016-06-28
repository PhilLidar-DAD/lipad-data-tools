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


class BulkUpload:
    
    def __init__(self, data_tiles_dir):
        
        # Init config from config.ini
        self.config = ConfigParser.ConfigParser()
        self.config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.ini"))
        
        # Init Ceph OGW settings 
        self.ceph_ogw = dict()
        self.ceph_sc = None
        options = self.config.options("ceph")
        for option in options:
            try:
                self.ceph_ogw[option] = self.config.get("ceph", option)
                if self.ceph_ogw[option] == -1:
                    print("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                self.ceph_ogw[option] = None
        
        # Activate virtualenv
        self.activate_venv()
        
        # Initialize metadata attributes
        self.metadata_logger = None
        self.metadata_log_file_path = None
        self.resume_dict = None
        
        # Setup log files
        self.header_line = "NAME,LAST_MODIFIED,SIZE_IN_BYTES,CONTENT_TYPE,FILE_HASH,GRID_REF"
        self.footer_line = "===END==="
        
        self.setup_logs()
        self.data_tiles_dir = data_tiles_dir
        self.setup_metadata_log()
            
    def activate_venv(self):
        #Try activating the virtualenv, error out if it cannot be activated
        try:
            activate_this_file = self.config.get("env", "activatethis")
            if not isfile(activate_this_file):
                raise Exception("ERROR: Failed to activate environment. Cannot find\n \
                                    virtualenv activate file in: [{0}]".format(activate_this_file))
            try:
                execfile(activate_this_file, dict(__file__=activate_this_file))
                from ceph_client import CephStorageClient
                import warnings, mimetypes, logging 
            except IOError as e:
                print "ERROR: Failed to activate environment. Check if virtualenv\n \
                         activate file is found in [{0}]".format(activate_this_file)
                raise e
        except ConfigParser.NoOptionError as e:
            print "No virtualenv activatethis setting found in config.ini, using default python environment. "
            
    
    def setup_logs(self):    
        directories = ["dump", "logs"]
        cwd = get_cwd()
        for d in directories:
            if not os.path.exists(join(cwd,d)):
                os.makedirs(join(cwd,d))
        logfiles = ["logs/ceph_storage.log"]
        for f in logfiles:  
            if not os.path.isfile(os.path.join(cwd, f)): 
                with open(os.path.join(cwd, f), 'wb') as temp_file:
                    temp_file.write("")
        
    def setup_metadata_log(self):
        # Set up metadata logger
        self.metadata_logger = logging.getLogger('MetadataLogger')
        self.metadata_logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        dir_tokens = filter(None, self.data_tiles_dir.split(os.path.sep))
        
        if len(dir_tokens) > 1:
            self.metadata_log_file_path = "dump/dt_{0}_{1}_{2}.inc".format(dir_tokens[-2], dir_tokens[-1], time.strftime("%Y-%m-%d-%H%M-%S"))
        else:
            self.metadata_log_file_path = "dump/dt_{0}_{1}.inc".format(dir_tokens[-1], time.strftime("%Y-%m-%d-%H%M-%S"))
            
        log_file_handler = logging.FileHandler(self.metadata_log_file_path)
        log_file_handler.setFormatter(logging.Formatter('%(message)s'))
        log_file_handler.setLevel(logging.INFO)

        self.metadata_logger.addHandler(log_file_handler)
        
        # Write metadata log header
        self.metadata_logger.info(self.header_line)
        
        
    def build_resume_dict(self, data_tiles_resume_log):
        csv_delimiter =','
        csv_columns   = 6
        self.resume_dict=OrderedDict()     # Use ordered dictionary to remember roder fo insertion
        filenames_list=[]
        
        # Check and read specified dump file to resume to
        if not os.path.isfile(data_tiles_resume_log):
            raise Exception("Metadata log file [{0}] is not a valid file!".format(data_tiles_resume_log))
        else:
            print "Resuming previous upload from metadata log file[{0}]".format(data_tiles_resume_log)
        
        # Index all previously uploaded files into list, using the filename as key
        with open(data_tiles_resume_log, "r") as resume_log:
            for csv_line in resume_log:
                if not csv_line == self.header_line:
                    metadata_list = csv_line.split(csv_delimiter) # Write each line to new metadata log as well
                    self.resume_dict[metadata_list[0]]=csv_line.rstrip()
                    
        print "Loaded [{0}] objects from previous metadata log file".format(len(self.resume_dict))
        
    def upload_data_tiles(self):
        # Connect to Ceph
        self.ceph_sc = CephStorageClient(   self.ceph_ogw['user'],
                                                self.ceph_ogw['key'],
                                                self.ceph_ogw['url'],
                                                container_name=self.ceph_ogw['container'])
        #Connect to Ceph Storage
        self.ceph_sc.connect()
        print "Connected to Ceph OGW at URI [{0}]".format(self.ceph_ogw['url'])

        # Parse list of allowed file extensions from config.ini
        allowed_files_exts = self.config.get("file_types", "allowed").replace(' ', '').split(',')
        
        print "Script will now upload files with the extensions {0}".format(allowed_files_exts)
        print "=====================================================================".format(allowed_files_exts)

        # Begin uploading data tiles
        for path, subdirs, files in walk(self.data_tiles_dir):
            for name in files:
                if (self.resume_dict is not None and name in self.resume_dict):
                    print "Skipping previously uploaded file [{0}]".format(join(path, name))
                    self.metadata_logger.info(self.resume_dict[name])
                else:
                #if (self.resume_dict is None) or (self.resume_dict is not None and name not in self.resume_dict):
                    # Upload each file
                    filename_tokens = name.rsplit(".")
                    
                    # Check if file is in allowed file extensions list 
                    if filename_tokens[-1] in allowed_files_exts:
                        grid_ref = filename_tokens[0].rsplit("_")[0]
                        file_path = join(path, name)
                        
                        try:
                            # Upload_file(file_path, grid_ref)
                            obj_dict = self.ceph_sc.upload_file_from_path(file_path)
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
                        except:
                            #DEBUG
                            obj_dict = dict()
                            obj_dict['name'] = name
                            obj_dict['last_modified'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #2016-03-23 14:59:18
                            obj_dict['bytes'] = os.path.getsize(file_path)
                            obj_dict['content_type'] = "#DEBUGTYPE"
                            obj_dict['hash'] = "#DEBUGHASH"
                            obj_dict['grid_ref'] = grid_ref
                            metadata_csv="{0},{1},{2},{3},{4},{5}".format(obj_dict['name'],
                                                                            obj_dict['last_modified'],
                                                                            obj_dict['bytes'],
                                                                            obj_dict['content_type'],
                                                                            obj_dict['hash'],
                                                                            obj_dict['grid_ref'])
                        self.metadata_logger.info(metadata_csv)
                        
        
        # Write metadata log footer
        self.metadata_logger.info(self.footer_line)
        
        # Rename completed log from .inc into .log
        new_metadata_log_file_path = ".".join(self.metadata_log_file_path.split('.')[:-1]) + ".log"
        os.rename(self.metadata_log_file_path, new_metadata_log_file_path)
        print "Ceph metadata logged at [{0}]".format(new_metadata_log_file_path)
        

if __name__ == "__main__": 
    
    # CLI Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("dir", 
                        help="Directory containing the tiled files and named according to their grid reference")
    parser.add_argument("-l", "--logfile",dest="logfile",
                        help="Path to resume log file for this upload")
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
