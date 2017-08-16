__version__ = "0.4.2"

import arcpy
import os
import time
import argparse
import logging

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

startTime = time.time()

# Parse arguments
parser = argparse.ArgumentParser(description='Updating of FHM Coverage')
parser.add_argument('-i','--input_directory')
parser.add_argument('-f','--fhm_coverage')
args = parser.parse_args()

LOG_FILENAME = "update_fhm_coverage.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("update_fhm_coverage.log")
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

input_directory = args.input_directory
fhm_coverage = args.fhm_coverage

for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in sorted(files):
		if f.endswith("shp") and f.__contains__("100"):
			fhm = os.path.join(path,f)
			rbfp_name = f.replace(".shp","")
			logger.info("Searching for fhm in input directory")

			try:
				# dissolve
				logger.info("Dissolving %s", rbfp_name)
				arcpy.Dissolve_management(fhm, r"in_memory\temp_dissolve")

				logger.info("Adding fields to dissolved fhm")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "UID", "SHORT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "FHM_SHP", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "RBFP", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "RBFP_COUNT", "SHORT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "RESOLUTION", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "SUC", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "PROCESSOR", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "AREA_SQKM", "DOUBLE")

				logger.info("Calculating field of dissolved fhm")
				arcpy.CalculateField_management(r"in_memory\temp_dissolve", "FHM_SHP",'"' + rbfp_name + '"', "PYTHON_9.3")

				logger.info("Appending dissolved fhm to fhm coverage")
				arcpy.Append_management(r"in_memory\temp_dissolve", fhm_coverage, "TEST")
				arcpy.Delete_management("in_memory")

			except Exception, e:
				logger.exception("Error in updating fhm coverage")

endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
