__version__ = "0.3"
__authors__ = "Jok Laurente"
__email__ = ["jmelaurente@gmail.com"]
__description__ = 'Validation of Flood Hazard Maps'

import os
import arcpy
import logging
import csv
import argparse
import time

startTime = time.time()

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
csv_file = open("validate_fhm.csv", 'wb')
spamwriter = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
spamwriter.writerow(['Filename', 'Path', 'SRID', 'Schema', 'Classes', 'Remarks'])

fhm_classes = [0,1,2,3]

for path, dirs, files in os.walk(input_directory, topdown=False):
	for f in files:
		if f.endswith(".shp"):
			try:
				var_field = False
				valid_classes = True
				fhm = os.path.join(path,f)

				# check spatial reference
				srid = arcpy.Describe(fhm).spatialReference.PCSCode
				logger.info("{0}: SRID: {1}".format(f,srid))

				# check schema
				fields = arcpy.ListFields(fhm)
				for field in fields:
					if field.name == 'Var':
						var_field = True

						# check classes
						cursor = arcpy.da.SearchCursor(fhm, "Var")
						for row in cursor:
							if row[0] not in fhm_classes:
								valid_classes = False
								logger.info("%s: Contains invalid class", f)
								break
						break

				if var_field:
					logger.info("%s: Var field exists", f)
				else:
					logger.info("%s: Var field doesn't exists", f)
					logger.info("%s: Contains invalid class", f)
					valid_classes =  False

				if valid_classes:
					logger.info("%s: All classes are valid", f)
				spamwriter.writerow([f, path, srid, var_field, valid_classes])
			except Exception:
				logger.info("%s: Cannot read shapefile", f)
				spamwriter.writerow([f, path, "N/A", "N/A", "N/A", "Cannot read shapefile"])
csv_file.close()
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
