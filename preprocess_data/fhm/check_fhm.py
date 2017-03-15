#!/usr/bin/python
# Salad VM

__version__ = "0.2.0"

import os
import ogr
import csv
import argparse
import time
from datetime import datetime

driver = ogr.GetDriverByName('ESRI Shapefile')
prj_32651 = r"PROJCS[WGS_1984_UTM_Zone_51N"
fhm_classes = [0,1,2,3]

# Start timing
startTime = time.time()

def parse_arguments():
	# Parse arguments
	parser = argparse.ArgumentParser(description='Check flood hazard maps')
	parser.add_argument('-i','--input_directory')
	parser.add_argument('-l','--logfile')
	args = parser.parse_args()
	return args

def check_prj(directory, logfile):
	for path, dirs, files in os.walk(directory,topdown=False):
		for fhm in sorted(files):
			if fhm.endswith(".shp"):
				path_fhm = os.path.join(path, fhm)
				ds_fhm = driver.Open(path_fhm)
				layer_fhm = ds_fhm.GetLayer()
				spatialRef_fhm = layer_fhm.GetSpatialRef()
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]", layer_fhm.GetName()
				prj_fhm = str(spatialRef_fhm).split(",",1)[0].replace('"',"")
				if prj_fhm != prj_32651:
					spamwriter.writerow([path_fhm, layer_fhm.GetName(), "Incorrect/Missing \
						projection"])
					print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]", \
					layer_fhm.GetName(), "Incorrect/Missing projection"
				check_schema(layer_fhm, path_fhm)

def check_schema(layer_fhm, path_fhm):
	layerDef_fhm = layer_fhm.GetLayerDefn()
	feature_count_fhm = layer_fhm.GetFeatureCount()
	schema = False
	# check if field Var exists
	for i in range(layerDef_fhm.GetFieldCount()):
		if layerDef_fhm.GetFieldDefn(i).GetName() == "Var":
			schema = True
			# check if dissolved
			if feature_count_fhm <= 4:
				for feature_fhm in layer_fhm:
					var_value = feature_fhm.GetFieldAsInteger("Var")

					# check if the values are accurate
					if var_value in fhm_classes:
						pass
					else:
						spamwriter.writerow([path_fhm, layer_fhm.GetName(), "Incorrect class"])
						print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]", \
						layer_fhm.GetName(), "Incorrect class"
			else:
				spamwriter.writerow([path_fhm, layer_fhm.GetName(), "More than 4 features"])
				print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]", \
				layer_fhm.GetName(), "More than 4 features"
			break
		else:
			pass
	if schema is False:
		spamwriter.writerow([path_fhm, layer_fhm.GetName(), "Incorrect schema"])
		print "[" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "]", \
		layer_fhm.GetName(), "Incorrect schema"

if __name__ == "__main__":
	args = parse_arguments()
	logfile = open(args.logfile, 'wb')
	spamwriter = csv.writer(logfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
	spamwriter.writerow(['Path', 'Hazard', 'Remarks'])
	check_prj(args.input_directory, logfile)
	endTime = time.time()  # End timing
	print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'