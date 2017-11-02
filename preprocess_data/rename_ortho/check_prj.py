#!/usr/bin/python2.7
import os
import subprocess
import csv

input_directory = "/mnt/pmsat-nas_geostorage/DPC/TERRA/Adjusted_Orthophotos/"
log_file = "ortho_results.csv"

csvfile = open(log_file, 'wb')
spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
spamwriter.writerow(["Path", "Tile", "Proj", "Remarks"])

for path, dirs, files in os.walk(input_directory,topdown=False):
	for ras in files:
		raster = os.path.join(path, ras)
		if ras.endswith('.tif'):
			proj = "None"
			try:
				cmd = "gdalsrsinfo {0} | grep PROJ.4".format(raster)
				print raster
				proj = subprocess.check_output(cmd, shell=True)
				spamwriter.writerow([path, ras, proj.replace("\n","")])
			except Exception, e:
				print e
				spamwriter.writerow([path, ras, proj.replace("\n",""), "Error"])
csvfile.close()
