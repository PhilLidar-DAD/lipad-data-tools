# Windows
# ArcPy

__version__ = "0.6"

import arcpy
import os
import sys
import time
import argparse
import csv
from datetime import datetime as dt

parser = argparse.ArgumentParser(description='Clip flood hazard maps per municipality')
parser.add_argument('-i','--input_directory')
args = parser.parse_args()

print "Python version:", sys.version
print "Script version:", __version__

print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
"Modules were successfully imported"

startTime = time.time()

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

if os.path.exists("clip_fhm_logs.csv"):
	csvfile = open("clip_fhm_logs.csv", 'ab')
	spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
else:
	csvfile = open("clip_fhm_logs.csv", 'ab')
	spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	spamwriter.writerow(['Riverbasin/Floodplain', 'Return Period','Province', 'City/Municipality','Geocode','Status', 'Timestamp'])

# define variables
# replace output directory
input_directory = args.input_directory
output_directory = r"\\pmsat-nas.prd.dream.upd.edu.ph\geostorage\DAD\FLOOD_HAZARD\Clipped_FHMs"
muni_boundary = r"\\pmsat-nas.prd.dream.upd.edu.ph\geostorage\DAD\FLOOD_HAZARD\FHM_Monitoring.gdb\PSA_Muni_Boundary"
muni_index= r"\\pmsat-nas.prd.dream.upd.edu.ph\geostorage\DAD\FLOOD_HAZARD\FHM_Monitoring.gdb\PSA_Muni_Index"

# muni boundary fields
muni_fields = ['REG_NAME', 'PRO_NAME', 'MUN_NAME', 'MUN_CODE', 'RB_FP', 'Return_Period', \
'Last_Clipped', 'Resolution', 'FHM_80_coverage', 'Builtup_coverage', 'Status', 'SHAPE@']

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

arcpy.MakeFeatureLayer_management(muni_index, muni_layer, "", "", \
				"OBJECTID_1 OBJECTID_1 VISIBLE NONE;Shape Shape VISIBLE NONE;REG_CODE REG_CODE \
				VISIBLE NONE;REG_NAME REG_NAME VISIBLE NONE;PRO_CODE PRO_CODE VISIBLE NONE;PRO_NAME \
				PRO_NAME VISIBLE NONE;MUN_CODE MUN_CODE VISIBLE NONE;MUN_NAME MUN_NAME VISIBLE \
				NONE;ISCITY ISCITY VISIBLE NONE;FHM_80_coverage FHM_80_coverage VISIBLE NONE;\
				Builtup_coverage Builtup_coverage VISIBLE NONE;Status Status VISIBLE NONE;\
				Shape_Length Shape_Length VISIBLE NONE;Shape_Area Shape_Area VISIBLE NONE")

# loop through the flood hazard shapefiles
for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in files:
		if f.endswith(".shp"):
			print "\n" + "#" * 70 + "\n"
			try:
				for_checking_value = True
				fhm_path = os.path.join(path,f)
				rbfp_name = f.split("_FH")[0]
				year = f.split("_FH")[1].split("yr")[0]
				resolution = f.split("_FH")[1].split("yr_")[1].replace(".shp","")
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"FHM path:", fhm_path
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Riverbasin/Floodplain name:", rbfp_name
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Return period:", year
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Resolution:", resolution

				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Selecting municipalities intersected with fhm"
				arcpy.SelectLayerByLocation_management(muni_layer, "INTERSECT", fhm_path,\
				 "", "NEW_SELECTION")

				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Performing analysis on selected municipalities"
				arcpy.Statistics_analysis(muni_layer, table, [["FHM_80_coverage", "SUM"], ["Builtup_coverage", "SUM"]])
				table_fields = ["SUM_FHM_80_coverage", "SUM_Builtup_coverage"]
				cursor1 = arcpy.da.SearchCursor(table, table_fields)
				for row1 in cursor1:
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Number of city/muni with 80% FHM coverage: ", str(row1[0])
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Number of city/muni with built-up areas: ", str(row1[1])

					if row1[0] > 0:
						for_checking_value = False
					if row1[1] > 0:
						for_checking_value = False
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:",\
					"FHM for checking: " , for_checking_value

				# loop through the selected muni
				cursor = arcpy.da.UpdateCursor(muni_layer, muni_fields)
				for row in cursor:
					fhm_exists = False
					region = row[0]
					province = row[1]
					muni = row[2]
					geocode = row[3]
					geom = row[11]

					coverage_80 = False
					builtup = False

					if row[8] == 1:
						coverage_80 = True
					if row[9] == 1:
						builtup = True

					print "\n" + "-" * 70 + "\n"

					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Region:", region
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Province:", province
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"City/Municipality:", muni
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"80% Coverage:", coverage_80
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Builtup COVERAGE:", builtup

					output_path_archive = os.path.join(output_directory, "FOR_ARCHIVING", region, province, muni)
					output_path_upload = os.path.join(output_directory, "FOR_UPLOADING", region, province, muni)

					if not os.path.exists(output_path_archive):
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Creating output directories"
						os.makedirs(output_path_archive)
					else:
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Output directory already exists"

					output_fhm = os.path.join(output_path_archive, geocode + "_FH" + year + "yr"\
					+ "_" + resolution + ".shp")

					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"FHM output:", output_fhm

					# check if fhm exists
					if os.path.exists(output_fhm):
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Flood hazard map already exists"
						fhm_exists = True

					if not fhm_exists:
						# clip flood hazard shapefile using muni index as extent
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Clipping flood hazard shapefile"
						arcpy.Clip_analysis(fhm_path, geom, temp_fhm, "")

						# erase the muni index extent using fhm to generate "area not assessed"
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Erasing muni index"
						arcpy.Erase_analysis(geom, temp_fhm, erase)

						# add fields to the temporary "area not assessed" shapefile
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Adding Var field to temporary 'area not assessed' shapefile"
						arcpy.AddField_management(erase, "Var", "SHORT")

						# calculate value of the temporary "area not assessed" shapefile 
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Calculating erase's Var field"
						arcpy.CalculateField_management(erase, "Var", "-1","PYTHON_9.3")

						# append the temporary "area not assessed" to fhm output
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending 'area not assessed' to fhm output"
						arcpy.Append_management(erase, temp_fhm, "NO_TEST")

						# generate temporary municipal outline
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Generating municipal outline"
						where_clause = "{0} = '{1}'".format('"MUN_CODE"', geocode)
						arcpy.Select_analysis(muni_boundary, select, where_clause)

						# add fields to the municpal outline shapefile
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Adding Var field to temporary municipal outline shapefile"
						arcpy.AddField_management(select, "Var", "SHORT")

						# calculate value of the temporary municipal outline shapefile 
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Calculating select's Var field"
						arcpy.CalculateField_management(select, "Var", "-2","PYTHON_9.3")

						# append the municipal outline to fhm output
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending municipal outline to fhm output"
						arcpy.Append_management(select, temp_fhm, "NO_TEST")

						# dissolve the fhm
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Dissolving fhm output"
						arcpy.Dissolve_management (temp_fhm, output_fhm, "Var")

					else:

						# create a duplicate copy of the old fhm except for municipal outline
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Creating a duplicate copy of the old fhm except for municipal outline"
						where_clause = "{0} <> {1}".format('"Var"', "-2")
						arcpy.Select_analysis(output_fhm, duplicate, where_clause)

						# delete the old fhm
						arcpy.Delete_management(output_fhm)
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Deleting the old fhm"

						# clip flood hazard shapefile using muni index as extent
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Clipping flood hazard shapefile"
						arcpy.Clip_analysis(fhm_path, geom, temp_fhm, "")

						# erase the duplicate fhm using clipped fhm"
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Erasing muni index"
						arcpy.Erase_analysis(duplicate, temp_fhm, erase)

						# append the erased fhm to to fhm output
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending 'area not assessed' to fhm output"
						arcpy.Append_management(erase, temp_fhm, "NO_TEST")

						##########################
						# generate temporary municipal outline
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Generating municipal outline"
						where_clause = "{0} = '{1}'".format('"MUN_CODE"', geocode)
						arcpy.Select_analysis(muni_boundary, select, where_clause)

						# add fields to the municpal outline shapefile
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Adding Var field to temporary municipal outline shapefile"
						arcpy.AddField_management(select, "Var", "SHORT")

						# calculate value of the temporary municipal outline shapefile 
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Calculating select's Var field"
						arcpy.CalculateField_management(select, "Var", "-2","PYTHON_9.3")

						# append the municipal outline to fhm output
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending municipal outline to fhm output"
						arcpy.Append_management(select, temp_fhm, "NO_TEST")
						##########################

						# dissolve the fhm
						print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Dissolving fhm output"
						arcpy.Dissolve_management (temp_fhm, output_fhm, "Var")

					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Adding Muncode field to fhm output"
					arcpy.AddField_management(output_fhm, "Muncode", "TEXT")

					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Calculating output fhm's Muncode"
					arcpy.CalculateField_management(output_fhm, "Muncode", '"' + geocode + '"', \
						"PYTHON_9.3")

					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Updating spatial index"
					arcpy.AddSpatialIndex_management(output_fhm)

					# check if fhm output is for uploading
					if coverage_80 or builtup:
						output_fhm_upload = output_fhm.replace("FOR_ARCHIVING","FOR_UPLOADING")
						if not os.path.exists(output_path_upload):
							print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
							"Creating output directories"
							os.makedirs(output_path_upload)
						else:
							print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
							"Output directory already exists"
						
						if os.path.exists(output_fhm_upload):
							print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
							"Deleting the old fhm"
							arcpy.Delete_management(output_fhm_upload)

					 	print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Copying output fhm to FOR_UPLOADING folder"
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
						row[5] = "FH" + year
					elif row[5].__contains__("FH" + year):
						pass
					else:
						row[5] = row[5] + ", " + "FH" + year

					# Resolution
					if row[7] is None:
						row[7] = resolution
					elif row[7].__contains__(resolution):
						pass
					else:
						row[7] = row[7] + ", " + resolution

					row[6] = dt.now()
					cursor.updateRow(row)

					spamwriter.writerow([rbfp_name, year, province, muni, geocode, row[10], dt.now().strftime('%Y-%m-%d %H:%M:%S')])

					# deleting temporary files
					print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Deleting in_memory workspace"
					arcpy.Delete_management("in_memory")

			except Exception, e:
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", str(e)
				spamwriter.writerow([rbfp_name, "", "", "", "", "ERROR:", dt.now().strftime('%Y-%m-%d %H:%M:%S')])
			
csvfile.close()
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'