# Windows
# ArcPy

__version__ = "0.5.0"

import arcpy
import os
import sys
import time
import argparse
from datetime import datetime

parser = argparse.ArgumentParser(description='Clip flood hazard maps per municipality')
parser.add_argument('-i','--input_directory')
args = parser.parse_args()

print "Python version:", sys.version
print "Script version:", __version__

print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
"Modules were successfully imported"

startTime = time.time()

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

current_date = str(datetime.now()).split(" ",1)[0]
log_file = open("logs_" + current_date + ".txt", "a")

# define variables
# replace output directory
input_directory = args.input_directory
output_directory = r"E:\FLOOD_HAZARD_MAPS\Testing\Clipped_FHM"
muni_boundary = r"E:\FLOOD_HAZARD_MAPS\Testing\Clip_FHM\FHM_Index.gdb\PSA_Muni_Boundary"

# muni boundary fields
muni_fields = ['REG_NAME', 'PRO_NAME', 'MUN_NAME', 'MUN_CODE', 'RB_FP', 'Return_Period', \
'Last_Clipped', 'Resolution', 'SHAPE@']

# temporary shapefiles
temp_erase = os.path.join(output_directory,"temp_erase.shp")
temp_select = os.path.join(output_directory,"temp_select.shp")
temp_fhm = os.path.join(output_directory,"temp_fhm.shp")
temp_diss = os.path.join(output_directory,"temp_diss.shp")

# layers
muni_layer = "muni_layer"
temp_fhm_layer = "temp_fhm_layer"

# lists
fhm_list_ok = []
fhm_list_err = []

arcpy.MakeFeatureLayer_management(muni_boundary, muni_layer, "", "", \
				"OBJECTID_1 OBJECTID_1 VISIBLE NONE;Shape Shape VISIBLE NONE;REG_CODE REG_CODE \
				VISIBLE NONE;REG_NAME REG_NAME VISIBLE NONE;PRO_CODE PRO_CODE VISIBLE NONE;PRO_NAME \
				PRO_NAME VISIBLE NONE;MUN_CODE MUN_CODE VISIBLE NONE;MUN_NAME MUN_NAME VISIBLE \
				NONE;ISCITY ISCITY VISIBLE NONE;Shape_Length Shape_Length VISIBLE NONE;Shape_Area \
				Shape_Area VISIBLE NONE")

# loop through the flood hazard shapefiles
for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in files:		
		if f.endswith(".shp"):
			print "\n" + "#" * 70 + "\n"
			try:
				fhm_path = os.path.join(path,f)
				rbfp_name = f.split("_FH")[0]
				year = f.split("_FH")[1].split("yr")[0]
				resolution = f.split("_FH")[1].split("yr_")[1].replace(".shp","")
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"FHM path:", fhm_path
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Riverbasin/Floodplain name:", rbfp_name
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Return period:", year
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Resolution:", resolution
				
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Selecting municipalities intersected with fhm"
				arcpy.SelectLayerByLocation_management(muni_layer, "INTERSECT", fhm_path,\
				 "", "NEW_SELECTION", "NOT_INVERT")

				cursor = arcpy.da.UpdateCursor(muni_layer, muni_fields)
				for row in cursor:
					fhm_exists = False
					region = row[0]
					province = row[1]
					muni = row[2]
					geocode = row[3]
					geom = row[8]

					print "\n" + "-" * 70 + "\n"

					output_path = os.path.join(output_directory, region, province, muni)
							
					if not os.path.exists(output_path):
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Creating output directories"
						os.makedirs(output_path)
					else:
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Output directory already exists"

					output_fhm = os.path.join(output_path, geocode + "_FH" + year + "yr"\
					+ "_" + resolution + ".shp")

					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"FHM output:", output_fhm

					# check if fhm exists
					if os.path.exists(output_fhm):
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Flood hazard map already exists"
						fhm_exists = True

					if not fhm_exists:
						# clip flood hazard shapefile using muni index as extent
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Clipping flood hazard shapefile"
						arcpy.Clip_analysis(fhm_path, geom, temp_fhm, "")

						# erase the muni index extent using fhm to generate "area not assessed"
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Erasing muni index"
						arcpy.Erase_analysis(geom, temp_fhm, temp_erase)
						
						# add fields to the temporary "area not assessed" shapefile
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Adding Var field to temporary 'area not assessed' shapefile"
						arcpy.AddField_management(temp_erase, "Var", "SHORT")
						
						# arcpy.AddField_management(temp_erase, "Muncode", "TEXT")
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Calculating temp_erase's Var field"
						arcpy.CalculateField_management(temp_erase, "Var", "-1","PYTHON_9.3")
						
						# append the "area not assessed" to fhm output
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending 'area not assessed' to fhm output" 
						arcpy.Append_management(temp_erase, temp_fhm, "NO_TEST")

						# dissolve the fhm
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Dissolving fhm output" 
						arcpy.Dissolve_management (temp_fhm, output_fhm, "Var")
						
					else:

						# create a duplicate copy of the old fhm
						arcpy.CopyFeatures_management(output_fhm, temp_fhm)
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Creating a duplicate copy of the old fhm"

						# delete the old fhm
						arcpy.Delete_management(output_fhm)
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Deleting the old fhm"

						# clip flood hazard shapefile using muni index as extent
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Clipping flood hazard shapefile"
						arcpy.Clip_analysis(fhm_path, geom, temp_diss, "")
			
						# erase the muni index extent using fhm to generate "area not assessed"
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Erasing muni index"
						arcpy.Erase_analysis(temp_fhm, temp_diss, temp_erase)

						# append the "area not assessed" to fhm output
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Appending 'area not assessed' to fhm output" 
						arcpy.Append_management(temp_erase, temp_diss, "NO_TEST")

						# dissolve the fhm
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
						"Dissolving fhm output" 
						arcpy.Dissolve_management (temp_diss, output_fhm, "Var")
					
					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Adding Muncode field to fhm output"
					arcpy.AddField_management(output_fhm, "Muncode", "TEXT")
					
					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Calculating output fhm's Muncode"
					arcpy.CalculateField_management(output_fhm, "Muncode", '"' + geocode + '"', \
						"PYTHON_9.3")

					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Updating spatial index"
					arcpy.AddSpatialIndex_management(output_fhm)

					# update index psa after clipping

					# Riverbasin/Floodplain name
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

					row[6] = datetime.now()
					cursor.updateRow(row)

					# deleting temporary files
					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
					"Deleting temporary shapefiles"
					arcpy.Delete_management(temp_erase)
					arcpy.Delete_management(temp_select)
					arcpy.Delete_management(temp_fhm)
					arcpy.Delete_management(temp_diss)
					arcpy.Delete_management(temp_fhm_layer)
					
				fhm_list_ok.append(fhm_path)
			except Exception, e:
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", e
				fhm_list_err.append(fhm_path)

print "List of flood hazard maps clipped"
log_file.write("List of flood hazard maps clipped\n")

for fhm_ok in fhm_list_ok:
	print fhm_ok
	log_file.write(fhm_ok + "\n")

print "\nList of flood hazard maps with error\n"
log_file.write("List of flood hazard maps with error\n")

for fhm_err in fhm_list_err:
	print fhm_err
	log_file.write(fhm_err + "\n")

log_file.close()
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
