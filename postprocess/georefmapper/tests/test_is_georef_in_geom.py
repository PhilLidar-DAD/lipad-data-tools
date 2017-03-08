#!/usr/bin/python
import sys, ast, os, argparse
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import fiona
from shapely.geometry.geo import shape, mapping
from fiona.crs import from_epsg
sys.path.append(os.path.dirname(os.path.realpath(__file__)).split('georefmapper')[0])
from georefmapper.mapper import georef_to_extents, extents_to_bbox, is_georef_in_geom
from georefmapper.index.utils import write_tile_to_shape, TILE_SIZE
from pprint import pprint

def parse_arguments():
    # Parse arguments
    parser = argparse.ArgumentParser(description="tests creation of tile/georefs from an input shapefile",
                                     epilog="Example: ./test_georef_tiles.py -i path/to/input_shapefile.shp -o path/to/output_shapefile.shp")
    parser.add_argument('georefs', metavar='N', type=str, nargs='+', 
                    help='georefs to be checks')
    
    parser.add_argument("-i", "--input", required=True,
                        help="Path to the input shapefile.")
    args = parser.parse_args()
    return args

if __name__=="__main__":
    args = parse_arguments()
    with fiona.open(args.input, 'r', 'ESRI Shapefile') as src_shp_fh:
        poly_list = []
        for data_feature in src_shp_fh:
            print "\nSHAPEFILE PROPERTIES:"
            pprint(data_feature['properties'])
        
            #get feature geometry
            df_geom = shape(data_feature['geometry'])
            
            if df_geom.type == 'Polygon':
                poly_list.append(df_geom)
            elif df_geom.type == 'MultiPolygon':
                poly_list.extend(list(df_geom))
        
        shp_mp = MultiPolygon(poly_list)
        
        print "\nEvaluating GeoRefs:"
        for georef in args.georefs:
            print " {0} : {1}".format(georef, is_georef_in_geom(georef, shp_mp))
            
            