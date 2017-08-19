__version__ = "0.1"
__authors__ = "Jok Laurente"
__email__ = ["jmelaurente@gmail.com"]
__description__ = 'Validation of Flood Hazard Maps'

import os
import arcpy
import logging
import csv
import argparse

LOG_FILENAME = "validate_fhm.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("validate_fhm.log")
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

parser = argparse.ArgumentParser(description='Validation of Flood Hazard Maps')
parser.add_argument('-i','--input_directory')
args = parser.parse_args()

input_directory = args.input_directory
csvfile = open("validate_fhm.csv", 'ab')
spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)

for path, dirs, files in os.walk(input_directory, topdown=False):
	for f in files:
		if f.endswith(".shp"):
			fhm = os.path.join(path,f)
			logger.info("%s: Checking spatial reference", f)
			srs = arcpy.Describe(fhm).spatialReference.PCSCode
			logger.info("{0}: {1}".format(f,srs))
