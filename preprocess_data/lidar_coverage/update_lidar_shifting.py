__version__ = "0.1.1"
__author__ = "Jok Laurente"
__email__ = "jmelaurente@gmail.com"
__description__ = "Script for updating LiDAR Coverage Shifting Values"

import arcpy
import xlrd
import os
import datetime
import logging
import csv
import argparse
import time
from xlrd import open_workbook

startTime = time.time()

# logging
LOG_FILENAME = "update_lidar_shifting.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("update_lidar_shifting.log")
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

# parse arguments
parser = argparse.ArgumentParser(description='Script for updating LiDAR Coverage Shifting Values')
parser.add_argument('-l','--lidar_coverage')
parser.add_argument('-s','--shifting_spreadsheet')
args = parser.parse_args()

lidar_coverage = args.lidar_coverage
metadata_spreadsheet = args.shifting_spreadsheet

lidar_fields = ["BLOCK_NAME", "X_SHIFT", "Y_SHIFT", "Z_SHIFT", "HEIGHT_DIFFERENCE", "RMSE_VAL", "PL1_SUC"]

csv_file = open("shifting_not_updated_.csv", 'wb')
spamwriter = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
spamwriter.writerow(['ID', 'Block', 'Remarks'])

# delete existing values in lidar coverage
for f in lidar_fields:
	if f != "BLOCK_NAME":
		arcpy.CalculateField_management(lidar_coverage, f, "None", "PYTHON_9.3")

for nrow in range(1, sheet.nrows):
	try:
		copied = False
		block_name = sheet.row(nrow)[0].value
		x_shift = sheet.row(nrow)[1].value
		y_shift = sheet.row(nrow)[2].value
		z_shift = sheet.row(nrow)[3].value
		height_difference = sheet.row(nrow)[4].value
		rmse = sheet.row(nrow)[5].value
		pl1_suc = sheet.row(nrow)[6].value

		logger.info("Checking if %s exists in LiDAR Coverage" % block_name)
		cursor = arcpy.da.UpdateCursor(lidar_coverage,lidar_fields)
		for row in cursor:
			if row[0] == block_name:
				logger.info("%s exists in LiDAR Coverage" % block_name)
				copied = True
				row[1] = x_shift
				row[2] = y_shift
				row[3] = z_shift
				row[4] = height_difference
				row[5] = rmse
				row[6] = pl1_suc
			logger.info("Successfully updated the shifting values of %s" % block_name)
			cursor.updateRow(row)
	except Exception, e:
		spamwriter.writerow([str(nrow), block_name, "Error"])
		logger.exception("Error in updating metadata of %s" % block_name)

	if not copied:
		logger.info("%s doesn't exists in LiDAR Coverage" % block_name)
		spamwriter.writerow([str(nrow), block_name, "Inconsistent name"])

csv_file.close()
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
