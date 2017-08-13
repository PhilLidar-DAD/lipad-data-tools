__version__ = "0.2.2"
__author__ = "Jok Laurente"
__email__ = "jmelaurente@gmail.com"
__description__ = "Script for updating LiDAR Coverage Metadata"

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

parser = argparse.ArgumentParser(description='Script for updating LiDAR Coverage Metadata')
parser.add_argument('-l','--lidar_coverage')
parser.add_argument('-m','--metadata_spreadsheet')
args = parser.parse_args()

lidar_coverage = args.lidar_coverage
metadata_spreadsheet = args.metadata_spreadsheet

lidar_fields = ["AREA", "BLOCK_NAME", "PROCESSOR", "SENSOR", "BASE_USED", "FLIGHT_NUMBER", "MISSION_NAME", "DATE_FLOWN", "IS_REMOVED"]

book = open_workbook(metadata_spreadsheet,on_demand=True)
sheet = book.sheet_by_name("Metadata")

LOG_FILENAME = "update_lidar_metadata.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

csv_file = open("blocks_not_updated.csv", 'wb')
spamwriter = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
spamwriter.writerow(['ID', 'Block', 'Remarks'])

logger = logging.getLogger("update_lidar_metadata.log")
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

def checkNull(field):
	if not field:
		return "NULL"
	else:
		return field

if __name__ == "__main__":
	for f in lidar_fields:
		if f != "BLOCK_NAME":
			arcpy.CalculateField_management(lidar_coverage, f, "None", "PYTHON_9.3")

	# loop thru the rows
	for nrow in range(1, sheet.nrows):
		try:
			copied = False
			block_name = sheet.row(nrow)[3].value
			area = checkNull(sheet.row(nrow)[1].value)
			processor = checkNull(sheet.row(nrow)[4].value)
			sensor = checkNull(sheet.row(nrow)[5].value)
			base_used = checkNull(sheet.row(nrow)[6].value)
			flight_number = checkNull(sheet.row(nrow)[7].value)
			mission_name = checkNull(sheet.row(nrow)[8].value)
			date_flown_dec = checkNull(sheet.row(nrow)[9].value)
			is_removed = checkNull(sheet.row(nrow)[12].value)

			logger.info("Checking if %s exists in LiDAR Coverage" % block_name)

			if date_flown_dec == "NULL":
				date_flown = "NULL"
			else:
				date_flown = str(datetime.datetime(*xlrd.xldate_as_tuple(date_flown_dec, book.datemode)).strftime("%m-%d-%Y"))

			cursor = arcpy.da.UpdateCursor(lidar_coverage,lidar_fields)
			for row in cursor:
				if row[1] == block_name:
					copied = True
					logger.info("%s exists in LiDAR Coverage" % block_name)
					logger.info("Checking if metadata exists")
					if row[5]:
						logger.info("Metadata already exists. Appending the values")
						row[0] = "{0} | {1}".format(row[0], area)
						row[2] = "{0} | {1}".format(row[2], processor)
						row[3] = "{0} | {1}".format(row[3], sensor)
						row[4] = "{0} | {1}".format(row[4], base_used)
						row[5] = "{0} | {1}".format(row[5], flight_number)
						row[6] = "{0} | {1}".format(row[6], mission_name)
						row[7] = "{0} | {1}".format(row[7], date_flown)
						row[8] = "{0} | {1}".format(row[8], is_removed)
					else:
						logger.info("Metadata doesn't exists. Updating the values")
						row[0] = area
						row[2] = processor
						row[3] = sensor
						row[4] = base_used
						row[5] = flight_number
						row[6] = mission_name
						row[7] = date_flown
						row[8] = is_removed
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
