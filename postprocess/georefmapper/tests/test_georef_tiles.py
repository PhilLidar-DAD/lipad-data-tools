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

def write_tile_to_shape_test(tile_extents,shp_file, tile_size, node_type):
    min_x, min_y, max_x, max_y = tile_extents
    tile_ulp = (min_x, max_y)
    tile_dlp = (min_x, min_y)
    tile_drp = (max_x, min_y)
    tile_urp = (max_x, max_y)
    gridref = "E{0}N{1}".format(min_x / tile_size, max_y / tile_size,)
    shp_file.write({
                #'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                'properties': {'GRID_REF': gridref,
                               'TYPE' : node_type,
                               'MINX' : min_x,
                               'MINY' : min_y,
                               'MAXX' : max_x,
                               'MAXY' : max_y,
                               'DSM' : 0,
                               'DTM' : 0,
                               'ORTHO' : 0,
                               'LAZ' : 0,
                               
                               },
            })

if __name__=="__main__":
    args = parse_arguments()
    schema = {  'geometry': 'Polygon',
                'properties': dict([('GRID_REF', 'str:254'), 
                                    ('TYPE', 'int:1'),
                                    ('MINX', 'float:19'), 
                                    ('MINY', 'float:19'), 
                                    ('MAXX', 'float:19'), 
                                    ('MAXY', 'float:19'),
                                    ('DSM', 'int:3'),
                                    ('DTM', 'int:3'),
                                    ('ORTHO', 'int:3'),
                                    ('LAZ', 'int:3'),])
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
#             out_shp_fh.write({
                    #'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
#                'geometry': mapping(df_geom),
#                'properties': {'GRID_REF': "FEATURE",
#                               'TYPE' : 0,
#                               'MINX' : 0.0,
#                               'MINY' : 0.0,
#                               'MAXX' : 0.0,
#                               'MAXY' : 0.0,
#                               },
#            })
                
            for georef in georef_list:
                write_tile_to_shape_test(mapper.georef_to_extents(georef), out_shp_fh, TILE_SIZE, 2)