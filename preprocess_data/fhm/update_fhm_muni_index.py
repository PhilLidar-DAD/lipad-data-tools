__version__ = "0.2.3"
__authors__ = "Jok Laurente"
__email__ = ["jmelaurente@gmail.com"]
__description__ = 'Updating of FHM Municipal Index'

import arcpy
import logging
import os
import time

startTime = time.time()

# parameters
arcpy.env.workspace = r"E:\FLOOD_HAZARD_MAPS\FHM_SHAPEFILES_FINAL\FHM_Monitoring.gdb"
muni_boundary = "psa_muni_boundary"
fhm_coverage = "fhm_coverage"
builtup = "builtup_all"
muni_index = "fhm_muni_index"

LOG_FILENAME = "update_fhm_muni_index.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.ERROR, format='%(asctime)s: %(levelname)s: %(message)s')

logger = logging.getLogger("update_fhm_muni_index.log")
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

# variables
fhm_muni_intersect = "in_memory/fhm_muni_intersect"
fhm_muni_dissolve = "in_memory/fhm_muni_dissolve"
fhm_muni_builtup = "in_memory/fhm_muni_builtup"
fhm_muni_point = "in_memory/fhm_muni_point"
fhm_muni_sj = "in_memory/fhm_muni_sj"

logger.info("Intersecting fhm coverage and muni boundary")
inFeatures = [fhm_coverage, muni_boundary]
arcpy.Intersect_analysis(inFeatures, fhm_muni_intersect, \
	"ALL", "", "INPUT")

logger.info("Dissolving fhm-muni")
arcpy.Dissolve_management(fhm_muni_intersect, fhm_muni_dissolve, "MUN_CODE;MUN_NAME;MUNI_AREA", "", "MULTI_PART", "DISSOLVE_LINES")

logger.info("Adding HAZ_AREA field to fhm-muni-dissolve")
arcpy.AddField_management(fhm_muni_dissolve, "HAZ_AREA", "DOUBLE")

logger.info("Adding HAZ_PERCENTAGE field to fhm-muni-dissolve")
arcpy.AddField_management(fhm_muni_dissolve, "HAZ_PERCENTAGE", "DOUBLE")

logger.info("Calculating the area of fhm-muni-dissolve")
arcpy.CalculateField_management(fhm_muni_dissolve, "HAZ_AREA", \
	"!shape.area@squarekilometers!", "PYTHON_9.3", "")

logger.info("Calculating the area percentage of fhm-muni-dissolve")
arcpy.CalculateField_management(fhm_muni_dissolve, "HAZ_PERCENTAGE",\
 "!HAZ_AREA! / !MUNI_AREA! * 100", "PYTHON_9.3", "")

logger.info("Intersecting fhm-muni-dissolve and builtup")
inFeatures2 = [fhm_muni_dissolve, builtup]
arcpy.Intersect_analysis(inFeatures2, fhm_muni_builtup, \
	"ALL", "", "INPUT")

logger.info("Joining fields of fhm-muni-dissolve to muni index")
arcpy.JoinField_management(muni_index, "MUN_CODE", fhm_muni_dissolve, "MUN_CODE", "HAZ_AREA;HAZ_PERCENTAGE")

logger.info("Joining fields of fhm-muni-builtup to muni index")
arcpy.JoinField_management(muni_index, "MUN_CODE", fhm_muni_builtup, "MUN_CODE", "FID_builtup_all")

codeblock_builtup = """def checkBuiltup(num):
	if num == 1:
		return "Y"
	else:
		return "N""""

logger.info("Calculating fields of muni index")
arcpy.CalculateField_management(muni_index, "HAZ_AREA", '!HAZ_AREA_1!', "PYTHON_9.3")
arcpy.CalculateField_management(muni_index, "HAZ_PERCENTAGE", '!HAZ_PERCENTAGE_1!', "PYTHON_9.3")
arcpy.CalculateField_management(muni_index, "IS_BUILTUP_COVERED", "checkBuiltup(!FID_builtup_all!)", "PYTHON_9.3", codeblock_builtup)

# compute boolean values
codeblock_percentage = """def checkPercentage(num):
	if num >= 80:
		return "Y"
	else:
		return "N""""

logger.info("Checking if fhm coverage is >= 80%")
arcpy.CalculateField_management(muni_index, "IS_FHM_COVERED", "checkPercentage(!HAZ_PERCENTAGE!)", "PYTHON_9.3", codeblock_percentage)

logger.info("Converting fhm-muni-intersect polygon to point")
arcpy.FeatureToPoint_management(fhm_muni_intersect, fhm_muni_point, "INSIDE")

mapping = "MUN_CODE \"MUN_CODE\" true true false 11 Text 0 0 ,First,#,{0},MUN_CODE,-1,-1;RBFP \"RBFP\" true true false 250 Text 0 0 ,Join,\",\",{1},RBFP,-1,-1;SUC \"SUC\" true true \
false 250 Text 0 0 ,Join,\",\",{1},SUC,-1,-1;PROCESSOR \"PROCESSOR\" true true false 250 Text 0 0 ,Join,\",\",{1},PROCESSOR,-1,-1".format(fhm_muni_dissolve, fhm_muni_point)

logger.info("Spatial joining fhm-muni-dissolve and fhm-muni-point")
arcpy.SpatialJoin_analysis(fhm_muni_dissolve, fhm_muni_point, fhm_muni_sj, "JOIN_ONE_TO_ONE", "KEEP_ALL", mapping, "CONTAINS", "", "")

logger.info("Joining fields of fhm-muni-sj to muni index")
arcpy.JoinField_management(muni_index, "MUN_CODE", fhm_muni_sj, "MUN_CODE", "RBFP;PROCESSOR;SUC")

logger.info("Sorting the values and removing the duplicates")
codeblock_sort = """def sortValues(txt):
	list_values = txt.split(",")
	list_values_sort = sorted(list(set(list_values)))
	return ','.join(list_values_sort)"""

arcpy.CalculateField_management(muni_index, "RBFP", "sortValues(!RBFP_1!)", "PYTHON_9.3", codeblock_sort)
arcpy.CalculateField_management(muni_index, "PROCESSOR", "sortValues(!PROCESSOR_1!)", "PYTHON_9.3", codeblock_sort)
arcpy.CalculateField_management(muni_index, "SUC", "sortValues(!SUC_1!)", "PYTHON_9.3", codeblock_sort)

logger.info("Calculating the number of rbfp")
arcpy.CalculateField_management(muni_index, "RBFP_COUNT", "!RBFP!.count(',') + 1", "PYTHON_9.3")

codeblock_resolution = """def checkResolution(fhm_res, index_res):
	proj_res = ""
	# 10m resolution
	if fhm_res == "10m":
		if index_res == "30m_10m":
			proj_res = "30m_10m"
		elif index_res == "30m":
			proj_res = "30m_10m"
		else:
			proj_res = "10m"

	# 30m resolution
	elif fhm_res == "30m":
		if index_res == "30m_10m":
			proj_res = "30m_10m"
		elif index_res == "10m":
			proj_res = "30m_10m"
		else:
			proj_res = "30m"
	else:
		proj_res = "30m_10m"
	return proj_res"""

logger.info("Calculating the projected resolution")
arcpy.MakeFeatureLayer_management(muni_index, "muni_index_layer")

# 10m
arcpy.MakeFeatureLayer_management(fhm_coverage, "fhm_coverage_10m_layer", "RESOLUTION = \'10m\'")
arcpy.SelectLayerByLocation_management("muni_index_layer", "INTERSECT", "fhm_coverage_10m_layer", "", "NEW_SELECTION")
arcpy.CalculateField_management("muni_index_layer", "PROJ_RESOLUTION", "checkResolution(\"10m\",!PROJ_RESOLUTION!)", "PYTHON_9.3", codeblock_resolution)

# 30m
arcpy.MakeFeatureLayer_management(fhm_coverage, "fhm_coverage_30m_layer", "RESOLUTION = \'30m\'")
arcpy.SelectLayerByLocation_management("muni_index_layer", "INTERSECT", "fhm_coverage_30m_layer", "", "NEW_SELECTION")
arcpy.CalculateField_management("muni_index_layer", "PROJ_RESOLUTION", "checkResolution(\"30m\",!PROJ_RESOLUTION!)", "PYTHON_9.3", codeblock_resolution)

# 30m_10m
arcpy.MakeFeatureLayer_management(fhm_coverage, "fhm_coverage_30m_10m_layer", "RESOLUTION = \'30m_10m\'")
arcpy.SelectLayerByLocation_management("muni_index_layer", "INTERSECT", "fhm_coverage_30m_10m_layer", "", "NEW_SELECTION")
arcpy.CalculateField_management("muni_index_layer", "PROJ_RESOLUTION", "checkResolution(\"30m_10m\",!PROJ_RESOLUTION!)", "PYTHON_9.3", codeblock_resolution)

drop_fields = ["HAZ_AREA_1", "HAZ_PERCENTAGE_1", "FID_builtup_all", "RBFP_1", "PROCESSOR_1", "SUC_1"]
arcpy.DeleteField_management(muni_index, drop_fields)

# delete intermediate data
arcpy.Delete_management("in_memory")

endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'
