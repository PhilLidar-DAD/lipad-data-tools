import subprocess
import ogr
import os
import shutil
import time
import math
import argparse
import sys

parser = argparse.ArgumentParser(description='Rename LAS/LAZ Files')
parser.add_argument('-i','--input_directory')
parser.add_argument('-o','--output_directory')
#parser.add_argument('-t','--type')
parser.add_argument("-ot", "--textfile")

args = parser.parse_args()
# Start timing
startTime = time.time()

inDir = args.input_directory
outDir = args.output_directory
#typeFile = args.type.upper()
#fileExtn = args.type.lower()

#if typeFile != "LAS" and typeFile != "LAZ":
#	print typeFile, 'is not a supported format'
#	sys.exit()

driver = ogr.GetDriverByName('ESRI Shapefile')

typeFile = "LAZ"

# Loop through the input directory
for path, dirs, files in os.walk(inDir,topdown=False):
	for las in files:
		if las.endswith(".laz"):
			try:
				ctr = 0
				inLAS = os.path.join(path, las)

				# Create temporary shapefile for LAZ's extent
				fullCmd = ' '.join(['lasboundary -i', inLAS, '-o temp.shp'])
				print '\n', fullCmd

				subprocess.call(fullCmd,shell=True)

				# Open the temporary shapefile
				inDS = driver.Open('temp.shp',0)
				inLayer = inDS.GetLayer()
				inFeat = inLayer.GetNextFeature()
				inGeom = inFeat.GetGeometryRef()
				inCentroid = inGeom.Centroid()

				inX = inCentroid.GetX()
				inY = inCentroid.GetY()
				
				print 'Centroid X', inX
				print 'Centroid Y', inY

				if inX % 1000 > 0:
					flrMinX = int(math.floor(inX * 0.001)*1000)	
				else:
					flrMinX = inX

				if inY % 1000 > 0:
					flrMaxY = int(math.floor(inY * 0.001)*1000)+1000		
				else:
					flrMaxY = inY

				minX = str(int(round(flrMinX*0.001)))
				maxY = str(int(round(flrMaxY*0.001)))

				print 'min X', minX
				print 'max Y', maxY

				outFN = ''.join(['E',minX,'N',maxY,'_',typeFile,'.',typeFile.lower()])
				outPath = os.path.join(outDir,outFN)

				# Check if output filename is already exists
				while os.path.exists(outPath):
					print '\nWARNING:', outPath, 'already exists!'
					ctr += 1
					outFN = ''.join(['E',minX,'N',maxY,'_',typeFile,'_',str(ctr),'.',typeFile.lower()])
					outPath = os.path.join(outDir,outFN)
				print os.path.join(path, las), outFN
				outTextfile = open(args.textfile, "a")
				outTextfile.write(os.path.join(path, las)+' --------- '+outFN+'\n')
				print outPath, 'copied successfully'
				shutil.copy(inLAS,outPath)
			except:
				outTextfile.write('Error while copying ' + inLAS + '\n')
			
outTextfile.close()
inDS.Destroy()
driver.DeleteDataSource('temp.shp')

endTime = time.time()  # End timing
print '\nElapsed Time:', str("{0:.2f}".format(round(endTime - startTime,2))), 'seconds'