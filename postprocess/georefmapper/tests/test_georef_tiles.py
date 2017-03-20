#!/usr/bin/python
import sys, ast, os, argparse
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import fiona
from shapely.geometry.geo import shape, mapping
from fiona.crs import from_epsg
sys.path.append(os.path.dirname(os.path.realpath(__file__)).split('georefmapper')[0])
from georefmapper import mapper
from georefmapper.index.utils import write_tile_to_shape, TILE_SIZE
from pprint import pprint

def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(description="tests creation of tile/georefs from an input shapefile",
                                     epilog="Example: ./test_georef_tiles.py \
-i path/to/input_shapefile.shp -o path/to/output_shapefile.shp")
    parser.add_argument("-i", "--input", required=True,
                        help="Path to the input shapefile.")
    parser.add_argument("-o", "--output", required=True,
                        help="Path to the output shapefile.")
    args = parser.parse_args()
    return args

if __name__=="__main__":
    args = parse_arguments()
    schema = {  'geometry': 'Polygon',
                'properties': dict([('GRID_REF', 'str:254'), ('TYPE', 'int:1'),('MINX', 'float:19'), ('MINY', 'float:19'), ('MAXX', 'float:19'), ('MAXY', 'float:19')])
                }
    print "\nINPUT: "+args.input
    print "OUTPUT: "+args.output
    with fiona.open(args.input, 'r', 'ESRI Shapefile') as src_shp_fh, fiona.open(args.output, 'w','ESRI Shapefile', schema, crs=from_epsg(32651), ) as out_shp_fh:
        
        for data_feature in src_shp_fh:
            
            print "\nSHAPEFILE PROPERTIES:"
            pprint(data_feature['properties'])
        
            #get feature geometry
            df_geom = shape(data_feature['geometry'])
            georef_list = mapper.georefs_in_geom(df_geom)
            
            #write feature to output
            out_shp_fh.write({
                    #'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                'geometry': mapping(df_geom),
                'properties': {'GRID_REF': "FEATURE",
                               'TYPE' : 0,
                               'MINX' : 0.0,
                               'MINY' : 0.0,
                               'MAXX' : 0.0,
                               'MAXY' : 0.0,
                               },
            })
                
            for georef in georef_list:
                write_tile_to_shape(mapper.georef_to_extents(georef), out_shp_fh, TILE_SIZE, 2)