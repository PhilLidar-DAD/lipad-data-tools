from shapely.geometry.geo import mapping, shape
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
import math

TILE_SIZE = 1000

def is_square(test_geom):
    if isinstance(test_geom, Polygon):
        if len(test_geom.exterior.coords) is 5:
            x_pts, y_pts = test_geom.exterior.coords.xy
            perimeter = zip(x_pts, y_pts)
            len_sides = []
            for i in range(len(perimeter)):
                len_sides.append(Point(perimeter[i]).distance(Point(perimeter[i-1])))
            len_sides.remove(0.0)
            if len(set(len_sides)) <= 1:
                #print ">>> SQUARE: "+ str(perimeter)+ " | SIDES: " + str(len_sides)
                return True
    
                
def write_tile_to_shape(tile_extents,shp_file, tile_size, node_type):
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
                               },
            })

def tile_floor(x):
    return int(math.floor(x / float(TILE_SIZE))) * TILE_SIZE

def tile_ceiling(x):
    return int(math.ceil(x / float(TILE_SIZE))) * TILE_SIZE

def get_tile_size(tile_extents):
    min_x, min_y, max_x, max_y = tile_extents
    dx = max_x-min_x
    dy = max_y-min_y
    if dx != dy:
        raise Exception("Not a square tile! dx={0}, dy={1}".format(dx,dy))
    else:
        return dx


def get_bounds_1x1km(extents):
    min_x, min_y, max_x, max_y = extents
    min_x = tile_floor(min_x)
    min_y = tile_floor(min_y)
    max_x = tile_ceiling(max_x)
    max_y = tile_ceiling(max_y)
    #print "1x1 EXTENTS: [{0},{1}],[{2},{3}]".format(min_x,min_y,max_x,max_y)
    return min_x, min_y, max_x, max_y

def get_pow2_extents(extents, tile_size):
    min_x, min_y, max_x, max_y = extents
    width_1x1km = int((max_x-min_x)/tile_size)
    height_1x1km = int((max_y-min_y)/tile_size)
    min_sqr_len = (1<<(max(height_1x1km,width_1x1km)-1).bit_length())*tile_size
    #print "P2SQR: [{0},{1}] - {2}".format(height_1x1km*tile_size, width_1x1km*tile_size, min_sqr_len)
    
    pow2_extents = (int(min_x),int(min_y),int(min_x+min_sqr_len),int(min_y+min_sqr_len))
    #write_tile_to_shape(pow2_extents,shp_file)
    return pow2_extents

def count_polygons(shp):
    count=0
    for feature in shp:
        geom=shape(feature['geometry'])
        print feature['properties']
        if geom.type == 'Polygon':
            count+=1
        elif geom.type == 'MultiPolygon':
            for part in geom:
                count+=1
        else:
            raise ValueError('Unhandled geometry type: ' + repr(geom.type))
        print count
    return count
