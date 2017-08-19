__version__ = "0.7.5"
__authors__ = "Jok Laurente"
__email__ = ["jmelaurente@gmail.com"]
__description__ = 'Clipping of Flood Hazard Maps per Municipality'

import arcpy
import os
import sys
import time
import argparse
import csv
import traceback
import logging
from datetime import datetime as dt

LOG_FILENAME = "clip_fhm.log"
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

parser = argparse.ArgumentParser(description='Clipping of Flood Hazard Maps per Municipality')
parser.add_argument('-i','--input_directory')
args = parser.parse_args()

startTime = time.time()

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

if os.path.exists("clip_fhm.csv"):
	csvfile = open("clip_fhm.csv", 'ab')
	spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
else:
	csvfile = open("clip_fhm.csv", 'ab')
	spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	spamwriter.writerow(['Riverbasin/Floodplain', 'Return Period', 'Resolution','Province', 'City/Municipality','Geocode','Status', 'Timestamp'])

# define variables
# replace output directory
input_directory = args.input_directory
output_directory = r"E:\FLOOD_HAZARD_MAPS\FHM_SHAPEFILES_FINAL\Clipped_FHMs"
muni_boundary = r"E:\FLOOD_HAZARD_MAPS\FHM_SHAPEFILES_FINAL\FHM_Monitoring.gdb\psa_muni_boundary"
muni_index= r"E:\FLOOD_HAZARD_MAPS\FHM_SHAPEFILES_FINAL\FHM_Monitoring.gdb\fhm_muni_index"

# muni boundary fields
muni_fields = ['REG_NAME', 'PRO_NAME', 'MUN_NAME', 'MUN_CODE', 'RBFP_CLIPPED', 'RETURN_PERIOD', \
'LAST_CLIPPED', 'RESOLUTION', 'IS_FHM_COVERED', 'IS_BUILTUP_COVERED', 'STATUS', 'PROJ_RESOLUTION', 'SHAPE@']

# temporary shapefiles
erase = r"in_memory\temp_erase"
select = r"in_memory\temp_select"
temp_fhm = r"in_memory\temp_fhm"
duplicate = r"in_memory\temp_duplicate"
table = r"in_memory\temp_table"

# layers
muni_layer = "muni_layer"
temp_fhm_layer = "temp_fhm_layer"

# lists
fhm_list_ok = []
fhm_list_err = []

logger.info("Creating a layer for Muni Index")
arcpy.MakeFeatureLayer_management(muni_index, muni_layer, "", "", \
				"OBJECTID OBJECTID VISIBLE NONE;Shape Shape VISIBLE NONE;REG_CODE REG_CODE \
				VISIBLE NONE;REG_NAME REG_NAME VISIBLE NONE;PRO_CODE PRO_CODE VISIBLE NONE;PRO_NAME \
				PRO_NAME VISIBLE NONE;MUN_CODE MUN_CODE VISIBLE NONE;MUN_NAME MUN_NAME VISIBLE \
				NONE;IS_CITY IS_CITY VISIBLE NONE;IS_FHM_COVERED IS_FHM_COVERED VISIBLE NONE;\
				IS_BUILTUP_COVERED IS_BUILTUP_COVERED VISIBLE NONE;STATUS STATUS VISIBLE NONE;\
				PROJ_RESOLUTION PROJ_RESOLUTION VISIBLE NONE\
				Shape_Length Shape_Length VISIBLE NONE;Shape_Area Shape_Area VISIBLE NONE")

# loop through the flood hazard shapefiles
for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in files:
		if f.endswith(".shp"):
			print "\n" + "#" * 70 + "\n"
			try:
				for_checking_value = True
				fhm_path = os.path.join(path,f)
				rbfp_name = f.split("_fh")[0]
				year = f.split("_fh")[1].split("yr")[0]
				resolution = f.split("_fh")[1].split("yr_")[1].replace(".shp","")
				logger.info("FHM path: {0}".format(fhm_path))
				logger.info("Riverbasin/Floodplain name: {0}".format(rbfp_name))
				logger.info("Return period: {0}".format(year))
				logger.info("Resolution: {0}".format(resolution))

				logger.info("Selecting municipalities intersected with fhm")
				arcpy.SelectLayerByLocation_management(muni_layer, "INTERSECT", fhm_path,\
				 "", "NEW_SELECTION")

				logger.info("Performing analysis on selected municipalities")
				arcpy.Statistics_analysis(muni_layer, table, [["IS_FHM_COVERED", "MAX"], ["IS_BUILTUP_COVERED", "MAX"]])
				table_fields = ["MAX_IS_FHM_COVERED", "MAX_IS_BUILTUP_COVERED"]
				cursor1 = arcpy.da.SearchCursor(table, table_fields)
				for row1 in cursor1:
					if row1[0] == "Y":
						for_checking_value = False
					if row1[1] == "Y":
						for_checking_value = False
					logger.info("FHM for checking: {0}".format(for_checking_value))

				# loop through the selected muni
				cursor = arcpy.da.UpdateCursor(muni_layer, muni_fields)
				for row in cursor:
					fhm_exists = False
					region = row[0]
					province = row[1]
					muni = row[2]
					geocode = row[3]
					proj_res = row[11]
					geom = row[12]

					coverage_80 = False
					builtup = False

					if row[8] == "Y":
						coverage_80 = True
					if row[9] == "Y":
						builtup = True

					print "\n" + "-" * 70 + "\n"

					logger.info("Region: {0}".format(region))
					logger.info("Province: {0}".format(province))
					logger.info("City/Municipality: {0}".format(muni))
					logger.info("80% Coverage: {0}".format(coverage_80))
					logger.info("Builtup COVERAGE: {0}".format(builtup))

					output_path_archive = os.path.join(output_directory, "FOR_ARCHIVING", region, province, muni)
					output_path_upload = os.path.join(output_directory, "FOR_UPLOADING", region, province, muni)

					if not os.path.exists(output_path_archive):
						logger.info("Creating output directories")
						os.makedirs(output_path_archive)
					else:
						logger.info("Output directory already exists")

					output_fhm = os.path.join(output_path_archive, geocode + "_fh" + year + "yr"\
					+ "_" + proj_res + ".shp")

					logger.info("FHM output: {0}".format(output_fhm))

					# check if fhm exists
					if os.path.exists(output_fhm):
						logger.info("Flood hazard map already exists")
						fhm_exists = True

					if not fhm_exists:
						# clip flood hazard shapefile using muni index as extent
						logger.info("Clipping flood hazard shapefile")
						arcpy.Clip_analysis(fhm_path, geom, temp_fhm, "")

						# erase the muni index extent using fhm to generate "area not assessed"
						logger.info("Erasing muni index")
						arcpy.Erase_analysis(geom, temp_fhm, erase)

						# add fields to the temporary "area not assessed" shapefile
						logger.info("Adding Var field to temporary 'area not assessed' shapefile")
						arcpy.AddField_management(erase, "Var", "SHORT")

						# calculate value of the temporary "area not assessed" shapefile
						logger.info("Calculating erase's Var field")
						arcpy.CalculateField_management(erase, "Var", "-1","PYTHON_9.3")

						# append the temporary "area not assessed" to fhm output
						logger.info("Appending 'area not assessed' to fhm output")
						arcpy.Append_management(erase, temp_fhm, "NO_TEST")

						# generate temporary municipal outline
						logger.info("Generating municipal outline")
						where_clause = "{0} = '{1}'".format('"MUN_CODE"', geocode)
						arcpy.Select_analysis(muni_boundary, select, where_clause)

						# add fields to the municpal outline shapefile
						logger.info("Adding Var field to temporary municipal outline shapefile")
						arcpy.AddField_management(select, "Var", "SHORT")

						# calculate value of the temporary municipal outline shapefile
						logger.info("Calculating select's Var field")
						arcpy.CalculateField_management(select, "Var", "-2","PYTHON_9.3")

						# append the municipal outline to fhm output
						logger.info("Appending municipal outline to fhm output")
						arcpy.Append_management(select, temp_fhm, "NO_TEST")

						# dissolve the fhm
						logger.info("Dissolving fhm output")
						arcpy.Dissolve_management (temp_fhm, output_fhm, "Var")

					else:

						# create a duplicate copy of the old fhm except for municipal outline
						logger.info("Creating a duplicate copy of the old fhm except for municipal outline")
						where_clause = "{0} <> {1}".format('"Var"', "-2")
						arcpy.Select_analysis(output_fhm, duplicate, where_clause)

						# delete the old fhm
						arcpy.Delete_management(output_fhm)
						logger.info("Deleting the old fhm")

						# clip flood hazard shapefile using muni index as extent
						logger.info("Clipping flood hazard shapefile")
						arcpy.Clip_analysis(fhm_path, geom, temp_fhm, "")

						# erase the duplicate fhm using clipped fhm"
						logger.info("Erasing muni index")
						arcpy.Erase_analysis(duplicate, temp_fhm, erase)

						# append the erased fhm to to fhm output
						logger.info("Appending 'area not assessed' to fhm output")
						arcpy.Append_management(erase, temp_fhm, "NO_TEST")

						# generate temporary municipal outline
						logger.info("Generating municipal outline")
						where_clause = "{0} = '{1}'".format('"MUN_CODE"', geocode)
						arcpy.Select_analysis(muni_boundary, select, where_clause)

						# add fields to the municpal outline shapefile
						logger.info("Adding Var field to temporary municipal outline shapefile")
						arcpy.AddField_management(select, "Var", "SHORT")

						# calculate value of the temporary municipal outline shapefile
						logger.info("Calculating select's Var field")
						arcpy.CalculateField_management(select, "Var", "-2","PYTHON_9.3")

						# append the municipal outline to fhm output
						logger.info("Appending municipal outline to fhm output")
						arcpy.Append_management(select, temp_fhm, "NO_TEST")
						##########################

						# dissolve the fhm
						logger.info("Dissolving fhm output")
						arcpy.Dissolve_management (temp_fhm, output_fhm, "Var")

					logger.info("Adding Muncode field to fhm output")
					arcpy.AddField_management(output_fhm, "Muncode", "TEXT")

					logger.info("Calculating output fhm's Muncode")
					arcpy.CalculateField_management(output_fhm, "Muncode", '"' + geocode + '"', \
						"PYTHON_9.3")

					logger.info("Updating spatial index")
					arcpy.AddSpatialIndex_management(output_fhm)

					# check if fhm output is for uploading
					if coverage_80 or builtup:
						output_fhm_upload = output_fhm.replace("FOR_ARCHIVING","FOR_UPLOADING")
						if not os.path.exists(output_path_upload):
							logger.info("Creating output directories")
							os.makedirs(output_path_upload)
						else:
							logger.info("Output directory already exists")

						if os.path.exists(output_fhm_upload):
							logger.info("Deleting the old fhm")
							arcpy.Delete_management(output_fhm_upload)

						logger.info("Copying output fhm to FOR_UPLOADING folder")
						arcpy.CopyFeatures_management(output_fhm, output_fhm_upload)

						row[10] = "FOR_UPLOADING"

					elif for_checking_value:
						row[10] = "FOR_CHECKING"
					else:
						row[10] = "FOR_ARCHIVING"

					# check if field is null
					if row[4] is None:
						row[4] = rbfp_name
					# check if rbfp name exists in field
					elif row[4].__contains__(rbfp_name):
						pass
					# append new rbfp clipped
					else:
						row[4] = row[4] + ", " + rbfp_name

					# Return period
					if row[5] is None:
						row[5] = "fh" + year
					elif row[5].__contains__("fh" + year):
						pass
					else:
						row[5] = row[5] + ", " + "fh" + year

					# Resolution
					if row[7] is None:
						row[7] = resolution
					elif row[7].__contains__(resolution):
						pass
					else:
						row[7] = row[7] + ", " + resolution

					row[6] = dt.now()
					cursor.updateRow(row)

					spamwriter.writerow([rbfp_name, year, resolution,province, muni, geocode, row[10], dt.now().strftime('%Y-%m-%d %H:%M:%S')])

					# deleting temporary files
					logger.info("Deleting in_memory workspace")
					arcpy.Delete_management("in_memory")

			except Exception, e:
				logger.exception(f)
				spamwriter.writerow([f, "", "", "", "", "", "ERROR", dt.now().strftime('%Y-%m-%d %H:%M:%S')])

				# deleting temporary files
				logger.info("Deleting in_memory workspace")
				arcpy.Delete_management("in_memory")

arcpy.Delete_management("in_memory")
csvfile.close()
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
