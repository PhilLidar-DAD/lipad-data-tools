# Windows
# ArcPy

__version__ = "0.1"

import arcpy
import os
import time
import logging
import argparse
from datetime import datetime as dt

startTime = time.time()

# logging
LOG_FILENAME = "update_lidar_coverage.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("clip_fhm.log")
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

# arguments
parser = argparse.ArgumentParser(description='Update LiDAR Coverage')
parser.add_argument('-i','--input_directory')
parser.add_argument('-t','--target_dataset')
args = parser.parse_args()

input_directory = args.input_directory
target_dataset = args.target_dataset
dissolve = r"in_memory/out_dissolve"

logger.info("Input directory: {0}".format(input_directory))
logger.info("Target dataset: {0}".format(target_dataset))

# loop through the MKP shapefiles
for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in files:
		if f.endswith(".shp"):
			try:
				print "\n" + "#" * 70 + "\n"
				mkp_shp = os.path.join(path,f)
				block_name = f.replace(".shp","")
				logger.info("Block Name: {0}".format(block_name))
				logger.info("Dissolving MKP Shapefile")
				arcpy.Dissolve_management(mkp_shp, dissolve)
				logger.info("Adding field to dissolved shapefile")
				arcpy.AddField_management(dissolve, "Block_Name", "TEXT")
				logger.info("Calculating field of dissolved shapefile")
				arcpy.CalculateField_management(dissolve, "Block_Name", "'{0}'".format(block_name),"PYTHON_9.3")
				#arcpy.CalculateField_management(dissolve, "Block_Name", "'" + block_name + "'","PYTHON_9.3")
				logger.info("Appending dissolved shapefile to target dataset")
				arcpy.Append_management(dissolve, target_dataset, "NO_TEST")

			except Exception:
				logger.exception(block_name)

			# deleting temporary files
			logger.info("Deleting in_memory workspace")
			arcpy.Delete_management("in_memory")
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'