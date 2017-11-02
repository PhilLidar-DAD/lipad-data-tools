__version__ = "0.8"

import arcpy
import os
import time
import argparse
import logging
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

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
temp_union2 = r"in_memory\temp_union2"
# specify static fhm directory
fhm_directory = ""
union_directory = os.path.join(input_directory, "UNION")
union_directory2 = os.path.join(input_directory, "UNION2")
fhm_coverage_layer = "fhm_coverage_layer"

def renameInput(fhm_input):
	fhm_bname = os.path.basename(fhm_input)
	fhm_dname = os.path.dirname(fhm_input)

	fhm_bname = fhm_bname.replace("(","___")
	fhm_bname = fhm_bname.replace(")","___")
	fhm_bname = fhm_bname.replace(", ","__")
	fhm_bname = fhm_bname.replace("-","_")

	fhm_input = os.path.join(fhm_dname, fhm_bname)
	return fhm_input

def unionFHM(fhm1, fhm2):
	bfhm1 = os.path.basename(fhm1)
	bfhm2 = os.path.basename(fhm2)
	code_block = """def getDominantValue(var1, var2):
	uvar = var1
	if var1 < var2:
		uvar = var2
	return uvar
	"""
	# Calculate dominant value of input fhm
	logger.info("%s: Performing union of fhm", bfhm1)
	arcpy.Identity_analysis(fhm1, fhm2, temp_union)
	logger.info("%s: Calculating the dominant value", bfhm1)
	arcpy.CalculateField_management(temp_union, "Var", "getDominantValue(!Var!, !Var_1!)", "PYTHON_9.3", code_block)

	# Calculate dominant value of intersected fhm
	arcpy.Identity_analysis(fhm2, fhm1, temp_union2)
	logger.info("%s: Calculating the dominant value", bfhm2)
	arcpy.CalculateField_management(temp_union2, "Var", "getDominantValue(!Var!, !Var_1!)", "PYTHON_9.3", code_block)

	logger.info("%s: Deleting old copy of fhm", bfhm1)
	logger.info("%s: Deleting old copy of fhm", bfhm2)
	arcpy.Delete_management(fhm1)
	arcpy.Delete_management(fhm2)

	logger.info("%s: Dissolving the feature", bfhm1)
	arcpy.Dissolve_management(temp_union, renameInput(fhm1), "Var")
	if renameInput(fhm1) != fhm1:
		logger.info("%s: Renaming the output file", bfhm1)
		arcpy.Rename_management(renameInput(fhm1),fhm1)

	logger.info("%s: Dissolving the feature", bfhm2)
	arcpy.Dissolve_management(temp_union2, renameInput(fhm2), "Var")
	if renameInput(fhm2) != fhm2:
		logger.info("%s: Renaming the output file", bfhm2)
		arcpy.Rename_management(renameInput(fhm2),fhm2)

if __name__ == "__main__":
	for path, dirs, files in os.walk(input_directory,topdown=False):
		fhm_list = []
		for f in sorted(files):
			try:
				if f.endswith("shp") and "UNION" not in path:
					fhm = os.path.join(path,f)
					if "fh100" in f:
						rbfp = f.split("_fh")[0]
						fh100_yr = f.replace(".shp","")

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
						# check if intersects
						arcpy.SelectLayerByLocation_management(fhm_coverage_layer, "INTERSECT", temp_dissolve)
						cursor = arcpy.da.SearchCursor(fhm_coverage_layer, "RBFP")

						logger.info("%s: Appending dissolved fhm to fhm coverage", rbfp)
						arcpy.Append_management(temp_dissolve, fhm_coverage, "TEST")

						# loop through the features of intersected layer
						for row in cursor:
							rbfp_value = row[0].lower()
							logger.info("{0}: Dissolved fhm intersects with {1}".format(rbfp, rbfp_value))

							# loop through the fhm shapefiles
							for path2, dirs2, files2 in os.walk(fhm_directory, topdown=False):
								for i in files2:
									if rbfp_value in i and i.endswith(".shp"):
										fhm_intersect = os.path.join(path2, i)
										fhm_list.append(fhm_intersect)
						for item in fhm_list:
							if "fh100" in item:
								unionFHM(fhm, item)
					elif "fh25" in f:
						for item in fhm_list:
							if "fh25" in item:
								unionFHM(fhm, item)
					elif "fh5" in f:
						for item in fhm_list:
							if "fh5" in item:
								unionFHM(fhm, item)

					logger.info("%s: Deleting intermediate files", rbfp)
					arcpy.Delete_management("in_memory")
					arcpy.Delete_management("fhm_coverage_layer")

			except Exception, e:
				logger.exception("Error in updating fhm coverage")

	endTime = time.time()  # End timing
	print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
