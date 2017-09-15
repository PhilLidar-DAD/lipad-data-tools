__version__ = "0.6"

import arcpy
import os
import time
import argparse
import logging

arcpy.env.outputZFlag = "Disabled"
arcpy.env.outputMFlag = "Disabled"

startTime = time.time()

# Parse arguments
parser = argparse.ArgumentParser(description='Updating of FHM Coverage')
parser.add_argument('-i','--input_directory')
parser.add_argument('-f','--fhm_coverage')
args = parser.parse_args()

LOG_FILENAME = "update_fhm_coverage.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("update_fhm_coverage.log")
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

input_directory = args.input_directory
fhm_coverage = args.fhm_coverage
temp_dissolve = r"in_memory\temp_dissolve"
temp_union = r"in_memory\temp_union"
fhm_directory = r"D:\JOK_WORKING\GITHUB\phil-lidar1-dad\lipad-data-tools\preprocess_data\fhm\FHM_RENAMED"
union_directory = os.path.join(input_directory, "UNION")
fhm_coverage_layer = "fhm_coverage_layer"

def unionFHM(fname, fhm1, fhm2):
	code_block = """def getDominantValue(var1, var2):
	uvar = var1
	if var1 < var2:
		uvar = var2
	return uvar
	"""
	fhm_union = os.path.join(union_directory, f)
	in_features = [fhm1, fhm2]
	logger.info("Performing union of fhm")
	arcpy.Union_analysis(in_features, temp_union)
	logger.info("Calculating the dominant value")
	arcpy.CalculateField_management(temp_union, "Var", "getDominantValue(!Var!, !Var_1!)", "PYTHON_9.3", code_block)
	logger.info("Dissolving the feature")
	arcpy.Dissolve_management(temp_union, fhm_union, "Var")
	arcpy.Delete_management("in_memory")

if __name__ == "__main__":
	for path, dirs, files in os.walk(input_directory,topdown=False):
		fhm_list = []
		for f in sorted(files):
			try:
				if f.endswith("shp") and "UNION" not in path:
					if "fh100" in f:
						fhm = os.path.join(path,f)
						rbfp = f.split("_fh")[0]
						fh100_yr = f.replace(".shp","")
						logger.info("Searching for fhm in input directory")
						# dissolve
						count = arcpy.GetCount_management(fhm_coverage)
						UID = int(count.getOutput(0)) + 1
						logger.info("%s: Dissolving fhm 100 year", rbfp)
						arcpy.Dissolve_management(fhm, temp_dissolve)

						logger.info("%s: Adding fields to dissolved fhm", rbfp)
						arcpy.AddField_management(temp_dissolve, "UID", "SHORT")
						arcpy.AddField_management(temp_dissolve, "FHM_SHP", "TEXT")
						arcpy.AddField_management(temp_dissolve, "RBFP", "TEXT")
						arcpy.AddField_management(temp_dissolve, "RBFP_COUNT", "SHORT")
						arcpy.AddField_management(temp_dissolve, "RESOLUTION", "TEXT")
						arcpy.AddField_management(temp_dissolve, "SUC", "TEXT")
						arcpy.AddField_management(temp_dissolve, "PROCESSOR", "TEXT")
						arcpy.AddField_management(temp_dissolve, "AREA_SQKM", "DOUBLE")

						logger.info("%s: Calculating fields of dissolved fhm", rbfp)
						arcpy.CalculateField_management(temp_dissolve, "UID", UID, "PYTHON_9.3")
						arcpy.CalculateField_management(temp_dissolve, "FHM_SHP",'"' + fh100_yr + '"', "PYTHON_9.3")
						arcpy.CalculateField_management(temp_dissolve, "RBFP",'"' + rbfp.title() + '"', "PYTHON_9.3")
						arcpy.CalculateField_management(temp_dissolve, "RBFP_COUNT", "!RBFP!.count(',') + 1", "PYTHON_9.3")
						arcpy.CalculateField_management(temp_dissolve, "RESOLUTION",'"10m"', "PYTHON_9.3")
						arcpy.CalculateField_management(temp_dissolve, "AREA_SQKM", "!shape.area@squarekilometers!", "PYTHON_9.3")

						# check if new fhm overlaps with existing coverage
						logger.info("%s: Checking if dissolved fhm intersects with existing coverage", rbfp)
						arcpy.MakeFeatureLayer_management(fhm_coverage, fhm_coverage_layer)
						arcpy.SelectLayerByLocation_management(fhm_coverage_layer, "INTERSECT", temp_dissolve)
						cursor = arcpy.da.SearchCursor(fhm_coverage_layer, "RBFP")

						# loop through the features of intersected layer
						for row in cursor:
							rbfp_value = row[0].lower()
							logger.info("{0}: Dissolved fhm intersects with {1}".format(rbfp, rbfp_value))

							# loop through the fhm shapefiles
							for path, dirs, files in os.walk(fhm_directory, topdown=False):
								for i in files:
									if rbfp_value in i and i.endswith(".shp"):
										fhm_intersect = os.path.join(path, i)
										fhm_list.append(fhm_intersect)
						for item in fhm_list:
							if "fh100" in item:
								print f, item
								unionFHM(f, fhm, item)
					elif "fh25" in f:
						for item in fhm_list:
							if "fh25" in item:
								print f, item
								unionFHM(f, fhm, item)
					elif "fh5" in f:
						for item in fhm_list:
							if "fh5" in item:
								print f, item
								unionFHM(f, fhm, item)

						# logger.info("%s: Appending dissolved fhm to fhm coverage", rbfp)
						# arcpy.Append_management(temp_dissolve, fhm_coverage, "TEST")
						arcpy.Delete_management("in_memory")
						arcpy.Delete_management("fhm_coverage_layer")

			except Exception, e:
				logger.exception("Error in updating fhm coverage")

	endTime = time.time()  # End timing
	print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
