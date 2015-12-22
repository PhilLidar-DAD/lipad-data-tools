#!/usr/bin/python2.7

import osgeotools
import argparse
import os, shutil
import logging

_version = "0.1.1"
print os.path.basename(__file__) + ": v" + _version


def _parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(epilog="Example: ./tile_dem.py \
-d data/MINDANAO1/d_mdn/ -t dsm -p WGS_84_UTM_zone_51N.prj -o output/")
    parser.add_argument("--version", action="version",
                        version=_version)
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("-o", "--orthophoto", required=False,
                        help="Path to orthophoto.")
    parser.add_argument("-p", "--prj_file", required=True,
                        help="Path to the projection file. Checks if the orthophoto's \
projection is the same.")
    parser.add_argument("-op", "--outputdir", required=True,
                        help="Path to output directory.")
    parser.add_argument("-d", "--directory", required=False,
            help="Path to input director for recursive operation")
    
    args = parser.parse_args()
    return args

def _rename_file(filepath, newName):
    dirName = os.path.dirname(filepath)
    fileName = os.path.basename(filepath)
    try:
        print "renaming %s at %s" % (fileName, dirName)
        os.rename(fileName, newName)
    except:
        print "failed to rename file %s. Check file ownership settings, perhaps?" % fileName

def _make_file_copy(filepath, target_dir, newname):
    try:
        shutil.copy(filepath, "%s%s" % (target_dir, newname))
    except:
        print "failed to create a copy to target dir. check permissions?"

    newcopy = "%s%s" % (target_dir,filepath)
    if os.path.exists(newcopy):
        print "%s created" % newcopy

    return newcopy

def _construct_new_filename(projection, orthophoto):
    #this is refactored main function
    _TILE_SIZE = 1000

    extension = orthophoto[-3:]

    # Open orthophoto
    orthophoto = osgeotools.open_raster(orthophoto, projection)
    print orthophoto["extents"]
    ul_x = orthophoto["extents"]["min_x"]
    ul_y = orthophoto["extents"]["max_y"]
    print "upper left:", ul_x, ul_y
    # Construct filename
    
    filename = "E%dN%d_ORTHO.%s" % (ul_x / _TILE_SIZE,
                                     ul_y / _TILE_SIZE, extension)

    return filename

def rename_ortho(orthophoto,outputdir,projection):
    nameWithoutExt = orthophoto.split('.')[0]   

    newname = _construct_new_filename(projection, orthophoto)
    newpath = _make_file_copy(orthophoto, outputdir,newname)
    print "%s created at output folder" % newname
    return newname
    
def rename_offset(orthophoto, outputdir):
    pass
    
    
def batch_rename(topdir, outputdir, projection):
    #walk the directory
    for path,dirs,files in os.walk(topdir,topdown=False):
        if path.endswith("Ortho"):
            for f in files:
                if f.endswith(".tif"):
                    ortho = os.path.join(path,f)
                    newname = rename_ortho(ortho, outputdir, projection)
                    #copy offset | hack
                    offset = ortho.replace('.tif', '.tfw')
                    offset_name = newname.replace('.tif', '.tfw')
                    _make_file_copy(offset, outputdir, offset_name)                  

if __name__ == '__main__':
    #TODO: output text to logger
    logging.basicConfig()

    # Parse arguments
    args = _parse_arguments()

    

    #newpath = _make_file_copy(args.orthophoto, args.outputdir)
    if args.orthophoto:
        filename = _construct_new_filename(args.prj_file, args.orthophoto)
        newpath = _make_file_copy(args.orthophoto, args.outputdir, filename)
        print "new filename:", filename

    if args.directory:
        print "batch processing"
    batch_rename(args.directory, args.outputdir, args.prj_file)


