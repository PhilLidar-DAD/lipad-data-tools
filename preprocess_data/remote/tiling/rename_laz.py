#!/usr/bin/python2.7

import subprocess
import ogr
import os
import shutil
import time
import math
import argparse
import sys
import logging
from tile_dem import _floor, _ceil


_version = "0.3.2"
print os.path.basename(__file__) + ": v" + _version
_logger = logging.getLogger()
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG

parser = argparse.ArgumentParser(description='Rename LAS/LAZ Files')
parser.add_argument("-v", "--verbose", action="count")
parser.add_argument('-i','--input_directory')
parser.add_argument('-o','--output_directory')
#parser.add_argument('-t','--type')
#parser.add_argument("-tmp", "--temp-dir", required=True,
#                        help="Path to temporary working directory.")
parser.add_argument("-l", "--logfile", required=True,
                        help="Filename of logfile")

args = parser.parse_args()
# Start timing
startTime = time.time()

inDir = args.input_directory
outDir = args.output_directory
#typeFile = args.type.upper()
#fileExtn = args.type.lower()

#if typeFile != "LAS" and typeFile != "LAZ":
#   print typeFile, 'is not a supported format'
#   sys.exit()

driver = ogr.GetDriverByName('ESRI Shapefile')


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
    fh = logging.FileHandler(args.logfile)
    fh.setLevel(_FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    _logger.addHandler(fh)

_setup_logging(args)

# Loop through the input directory
for path, dirs, files in os.walk(inDir,topdown=False):
    for las in files:
        if las.endswith(".laz") or las.endswith(".las"):
            typeFile = las.split(".")[-1].upper()
            ctr = 0
            laz_file_path = os.path.join(path, las)

            # get LAZ bounding box/extents
            p = subprocess.Popen(['./lasbb', '-get_bb', laz_file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = p.communicate()
            returncode = p.returncode
            if returncode is 0:
                tokens = out.split(" ")
                minX = float(tokens[1])
                minY = float(tokens[2])
                maxX = float(tokens[3])
                maxY = float(tokens[4])
                
                bbox_center_x = (minX+(maxX-minX)/2) 
                bbox_center_y = (minY+(maxY-minY)/2)
                
                _TILE_SIZE=1000
                tile_x =  int(math.floor(bbox_center_x / float(_TILE_SIZE)))
                tile_y =  int(math.floor(bbox_center_y / float(_TILE_SIZE))) + 1

                #outFN = ''.join(['E',tile_x,'N',tile_y,'_',typeFile,'.',typeFile.lower()])
                outFN = 'E{0}N{1}_{2}.{3}'.format(tile_x, tile_y, typeFile, typeFile.lower())
                outPath = os.path.join(outDir,outFN)

                # Check if output filename is already exists
                while os.path.exists(outPath):
                    print '\nWARNING:', outPath, 'already exists!'
                    ctr += 1
                    #outFN = ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
                    outFN = 'E{0}N{1}_{2}_{3}.{4}'.format(tile_x, tile_y, typeFile, str(ctr), typeFile.lower())
                    outPath = os.path.join(outDir,outFN)
                print os.path.join(path, las), outFN
                
                _logger.info(os.path.join(path, las)+' --------- '+outFN+'\n')
                
                print outPath, 'copied successfully'
                shutil.copy(laz_file_path, outPath)
            else:
                _logger.error("Error reading extents of [{0}]. Trace from lasbb:\n{1}".format(laz_file_path, out))
                
            
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
