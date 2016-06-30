import os, subprocess
import osgeotools
import math
import argparse
from datetime import datetime
_TILE_SIZE = 1000

_version = "0.1.0"

def _floor(x):
    return int(math.floor(x / float(_TILE_SIZE)) * _TILE_SIZE)

def _ceil(x):
    return int(math.ceil(x / float(_TILE_SIZE)) * _TILE_SIZE)

def get_dir_size(dir_path = '.'):
    cli_cmd="du -s "+dir_path
    p = subprocess.Popen(cli_cmd.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return out.split('\t')[0]

def find_fmc_dtms(toplevel_dir, output_log_file, prj_file, from_date=None):
    from_dt = None
    if from_date is not None:
        from_dt = datetime.strptime(from_date, "%Y/%m/%d")
    with open(output_log_file, 'w') as log_fh:
        for dem_dir, dirs, files in os.walk(toplevel_dir):
            for name in files:
                time_created = datetime.fromtimestamp(os.path.getctime(dem_dir))
                if from_date is None or time_created > from_dt:
                    if name == "hdr.adf":
                        time_created = datetime.fromtimestamp(os.path.getctime(dem_dir)).strftime("%Y/%m/%d")
                        time_modded = datetime.fromtimestamp(os.path.getmtime(dem_dir)).strftime("%Y/%m/%d")
                        #print "[{0}] -  {1}, None".format(time_created, dem_dir)
                        print "[{0} || {1}] -  {2}, None".format(time_created, time_modded, os.path.join(dem_dir,name))
                        try:
                            dem = osgeotools.open_raster(dem_dir, prj_file)
                            tile_extents = [_floor(dem["extents"]["min_x"]),
                                            _floor(dem["extents"]["min_y"]),
                                            _ceil(dem["extents"]["max_x"]),
                                            _ceil(dem["extents"]["max_y"])]
                            time_created = datetime.fromtimestamp(os.path.getctime(dem_dir)).strftime("%Y/%m/%d")
                            status ="NEW"
                            rb_name = dem_dir.split(os.sep)[8]
                            log_fh.write("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, None\n".format(time_created, 
                                                                                                        dem_dir, 
                                                                                                        status, 
                                                                                                        rb_name, 
                                                                                                        get_dir_size(dem_dir), 
                                                                                                        tile_extents[0], tile_extents[1], tile_extents[2], tile_extents[3]))
                        except Exception as e:
                            #log_fh.write("{0}, {1} ERROR, {2}\n".format(time_created, dem_dir, e))
                            status ="ERROR"
                            log_fh.write("{0}, {1}, {2}, {3}, {4}, , , , , {5}\n".format(time_created, 
                                                                                            dem_dir, 
                                                                                            status, 
                                                                                            rb_name, 
                                                                                            get_dir_size(dem_dir), 
                                                                                            e))

def test_find_fmc_dtm(toplevel_dir, from_date):
    for dem_dir, dirs, files in os.walk(toplevel_dir):
        for name in files:
            if name == "hdr.adf":
                #time_created = datetime.fromtimestamp(os.path.getctime(os.path.join(dem_dir,name))).strftime("%Y/%m/%d")
                #time_created = datetime.fromtimestamp(os.path.getctime(os.path.join(dem_dir,name)))
                time_created = datetime.fromtimestamp(os.path.getctime(dem_dir))
                from_dt = datetime.strptime(from_date, "%Y/%m/%d")
                if time_created > from_dt:
                    print "[{0}] -  {1}, None\n".format(time_created, os.path.join(dem_dir))

"""
toplevel_dir="/mnt/geostorage/EXCHANGE/DPC/MISCELLANEOUS/For_FMC"
output_log_file="/home/AD/autotiler/fmc_dtms_20160603.log"
prj_file="/home/AD/autotiler/scripts/tile_dem/WGS_84_UTM_zone_51N.prj"
from_date="2016/06/02"

find_fmc_dtms(toplevel_dir, output_log_file, prj_file, from_date)

"""

if __name__ == '__main__':
    pass