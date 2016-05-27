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
parser.add_argument("-tmp", "--temp-dir", required=True,
                        help="Path to temporary working directory.")
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

typeFile = "LAZ"
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
        if las.endswith(".laz"):
            try:
                ctr = 0
                inLAS = os.path.join(path, las)

                # Create temporary shapefile for LAZ's extent
                fullCmd = ' '.join(['lasboundary -i', inLAS, '-o {0}'.format(os.path.join(args.temp_dir,'temp_laz.shp'))])
                print '\n', fullCmd

                subprocess.call(fullCmd,shell=True)

                # Open the temporary shapefile
                inDS = driver.Open(os.path.join(args.temp_dir,'temp_laz.shp'),0)
                inLayer = inDS.GetLayer()
                inFeat = inLayer.GetNextFeature()
                inGeom = inFeat.GetGeometryRef()
                inCentroid = inGeom.Centroid()

                inX = inCentroid.GetX()
                inY = inCentroid.GetY()
                
                print 'Centroid X', inX
                print 'Centroid Y', inY

                if inX % 1000 > 0:
                    flrMinX = int(math.floor(inX * 0.001)*1000) 
                else:
                    flrMinX = inX

                if inY % 1000 > 0:
                    flrMaxY = int(math.floor(inY * 0.001)*1000)+1000        
                else:
                    flrMaxY = inY

                minX = str(int(round(flrMinX*0.001)))
                maxY = str(int(round(flrMaxY*0.001)))

                print 'min X', minX
                print 'max Y', maxY

                outFN = ''.join(['E',minX,'N',maxY,'_',typeFile,'.',typeFile.lower()])
                outPath = os.path.join(outDir,outFN)

                # Check if output filename is already exists
                while os.path.exists(outPath):
                    print '\nWARNING:', outPath, 'already exists!'
                    ctr += 1
                    outFN = ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
                    outPath = os.path.join(outDir,outFN)
                print os.path.join(path, las), outFN
                
                _logger.info(os.path.join(path, las)+' --------- '+outFN+'\n')
                
                print outPath, 'copied successfully'
                shutil.copy(inLAS,outPath)
            except:
                _logger.error('Error while copying ' + inLAS + '\n')
            
inDS.Destroy()
driver.DeleteDataSource(os.path.join(args.temp_dir,'temp_laz.shp'))

endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
