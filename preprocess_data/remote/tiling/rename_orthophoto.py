#!/usr/bin/python2.7

import osgeotools
import argparse
import os, shutil
import logging
import sys

_version = "0.2.0"
print os.path.basename(__file__) + ": v" + _version
_logger = logging.getLogger()
_LOG_LEVEL = logging.DEBUG
_CONS_LOG_LEVEL = logging.INFO
_FILE_LOG_LEVEL = logging.DEBUG
_TILE_SIZE = 1000

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
    parser.add_argument("-rbid", "--rbid", required=True,
                        help="River basin ID for this DEM.")
    parser.add_argument("-l", "--logfile", required=True,
                        help="Filename of logfile")
    args = parser.parse_args()
    return args

def _setup_logging(args):
    # Setup logging
    _logger.setLevel(_LOG_LEVEL)
    formatter = logging.Formatter("[%(asctime)s] %(filename)s \
(%(levelname)s,%(lineno)d) : %(message)s")

    # Check verbosity for console
    if args.verbose and args.verbose >= 1:
        global _CONS_LOG_LEVEL
        _CONS_LOG_LEVEL = logging.DEBUG
    else:
        _CONS_LOG_LEVEL = logging.INFO

    # Setup console logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_CONS_LOG_LEVEL)
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    # Setup file logging
    fh = logging.FileHandler(args.logfile)
    fh.setLevel(_FILE_LOG_LEVEL)
    fh.setFormatter(formatter)
    _logger.addHandler(fh)
    
def _rename_file(filepath, newName):
    dirName = os.path.dirname(filepath)
    fileName = os.path.basename(filepath)
    try:
        _logger.info("renaming %s at %s" % (fileName, dirName))
        os.rename(fileName, newName)
    except:
        _logger.error("failed to rename file %s. Check file ownership settings, perhaps?" % fileName)

def _make_file_copy(filepath, target_dir, newname):
    try:
        tile_name = newname
        dup_ctr=0
        while os.path.exists(os.path.join(target_dir, tile_name)):
            dup_ctr+=1
        if dup_ctr > 0:
            tile_name = "{0}_{1}".format(tile_name,dup_ctr)
        shutil.copy(filepath, os.path.join(target_dir, tile_name))
    except:
        _logger.error("failed to create a copy to target dir. check permissions?")

    newcopy = os.path.join(target_dir,filepath)
    if os.path.exists(newcopy):
        _logger.debug("%s created" % newcopy)

    return newcopy

def _construct_new_filename(projection, orthophoto):
    #this is refactored main function
    extension = orthophoto[-3:]

    # Open orthophoto
    orthophoto = osgeotools.open_raster(orthophoto, projection)
    _logger.debug("Extents: {0}".format(orthophoto["extents"]))
    ul_x = orthophoto["extents"]["min_x"]
    ul_y = orthophoto["extents"]["max_y"]
    _logger.debug("upper left: {0},{1}".format(ul_x, ul_y))
    # Construct filename
    
    filename = "E%dN%d_ORTHO_%s.%s" % (ul_x / _TILE_SIZE,
                                     ul_y / _TILE_SIZE, 
                                     args.rbid.upper(), 
                                     extension)

    return filename

def rename_ortho(orthophoto,outputdir,projection):
    nameWithoutExt = orthophoto.split('.')[0]   

    newname = _construct_new_filename(projection, orthophoto)
    newpath = _make_file_copy(orthophoto, outputdir,newname)
    _logger.info("%s created at output folder" % newname)
    return newname
    
def rename_offset(orthophoto, outputdir):
    pass
    
    
def batch_rename(topdir, outputdir, projection):
    #walk the directory
    for path,dirs,files in os.walk(topdir,topdown=False):
        for f in files:
            if f.endswith(".tif"):
                ortho = os.path.join(path,f)
                try:
                    newname = rename_ortho(ortho, outputdir, projection)
                    #copy offset | hack
                    offset = ortho.replace('.tif', '.tfw')
                    offset_name = newname.replace('.tif', '.tfw')
                    _make_file_copy(offset, outputdir, offset_name)
                except RuntimeError:
                    _logger.error("Failed to read Orthophoto [{0}]. Did not create {1}.".format(os.path.join(path,f),newname))

if __name__ == '__main__':
    
    # Parse arguments
    args = _parse_arguments()
    _setup_logging(args)
    

    #newpath = _make_file_copy(args.orthophoto, args.outputdir)
    if args.orthophoto:
        filename = _construct_new_filename(args.prj_file, args.orthophoto)
        newpath = _make_file_copy(args.orthophoto, args.outputdir, filename)
        _logger.info("new filename:" + filename)

    if args.directory:
        _logger.info("batch processing")
    batch_rename(args.directory, args.outputdir, args.prj_file)


