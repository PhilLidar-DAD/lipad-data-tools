# Windows
# ArcPy

__version__ = "0.2"

import arcpy
import os
import time
from datetime import datetime as dt

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

startTime = time.time()

# parameters
input_directory = r""
fhm_coverage = r""
list_err = []

for path, dirs, files in os.walk(input_directory,topdown=False):
	for f in sorted(files):
		if f.endswith("shp") and f.__contains__("100"):
			fhm = os.path.join(path,f)
			rbfp_name = f.replace(".shp","")
			print "#" * 80
			print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
			"FHM:", fhm
			print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
			"RBFP Name:", rbfp_name

			try:
				# dissolve
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Dissolving fhm"
				arcpy.Dissolve_management(fhm, r"in_memory\temp_dissolve")

				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Adding fields to dissolved fhm"
				arcpy.AddField_management(r"in_memory\temp_dissolve", "RBFP_shp", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "RBFP_name", "TEXT")
				arcpy.AddField_management(r"in_memory\temp_dissolve", "Processor", "TEXT")

				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Calculating field of dissolved fhm "
				arcpy.CalculateField_management(r"in_memory\temp_dissolve", "RBFP_shp",'"' + rbfp_name + '"', "PYTHON_9.3")

				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", \
				"Appending dissolved fhm to fhm coverage"
				arcpy.Append_management(r"in_memory\temp_dissolve", fhm_coverage, "TEST")
				arcpy.Delete_management("in_memory")

			except Exception, e:
				print "[" + dt.now().strftime('%Y-%m-%d %H:%M:%S') + "]:", e
				list_err.append(fhm + "\n")

print list_err
endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
