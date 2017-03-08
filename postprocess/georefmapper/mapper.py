import fiona
from shapely.geometry.geo import shape, mapping
from pprint import pprint
from shapely.geometry.polygon import Polygon
from .index.modquadtree import Node, QuadTree
from .index.utils import get_bounds_1x1km, get_pow2_extents, write_tile_to_shape, get_tile_size, TILE_SIZE 
from fiona.crs import from_epsg


LIDAR_COVERAGE="/home/ken/LAStools/shapefiles/lidar_coverage/lidar_coverage.shp"
OUTPUT_DIR="/home/ken/LAStools/shapefiles/lidar_coverage/object_shps"
COUNT_LIMIT = 2000

class InvalidGeorefException(Exception):
    pass


def georef_to_extents(georef):
    try:
        parts = georef.lower().split('n')
        min_x = int(parts[0].replace('e',''))*TILE_SIZE
        max_x = min_x + TILE_SIZE
        max_y = int(parts[1])*TILE_SIZE
        min_y = max_y - TILE_SIZE
        return [min_x, min_y, max_x, max_y]
    except ValueError:
        raise InvalidGeorefException("Invalid GeoRef: "+georef)
    except (AttributeError, TypeError):
        raise InvalidGeorefException("GeoRef must be a string: "+str(georef))
    except IndexError:
        raise InvalidGeorefException("Missing Easting/Northing indicator: "+str(georef))
    
def extents_to_georef(extents):
    min_x, min_y, max_x, max_y = extents
    return "E{0}N{1}".format(int(min_x / TILE_SIZE), int(max_y / TILE_SIZE),)

def extents_to_bbox(extents):
    min_x, min_y, max_x, max_y = extents
    tile_ulp = (min_x, max_y)
    tile_dlp = (min_x, min_y)
    tile_drp = (max_x, min_y)
    tile_urp = (max_x, max_y)
    return Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])

def is_georef_in_geom(georef, shapely_geometry_32651):
    tile_poly = extents_to_bbox(georef_to_extents(georef))
    poly_int =  tile_poly.intersection(shapely_geometry_32651)
    
    if poly_int.is_empty:
        return False
    else:
        return True

def georefs_in_geom(shapely_geometry_32651):
    pow2_bounds = get_pow2_extents(get_bounds_1x1km(shapely_geometry_32651.bounds), TILE_SIZE)
    rootrect = list(pow2_bounds)
    rootnode = Node(None, rootrect)
    tree = QuadTree(rootnode, TILE_SIZE, shapely_geometry_32651)
    georef_list = []
    for leaf in tree.get_leaf_nodes():
        leaf_tile_size = get_tile_size(leaf.rect)
        min_x, min_y, max_x, max_y = leaf.rect
        if leaf_tile_size > TILE_SIZE:
            for tile_y in xrange(min_y + TILE_SIZE,
                         max_y + TILE_SIZE,
                         TILE_SIZE):
                for tile_x in xrange(min_x,
                                     max_x,
                                     TILE_SIZE):
                    
                    t_min_x = tile_x
                    t_max_x = tile_x +TILE_SIZE
                    t_min_y = tile_y -TILE_SIZE
                    t_max_y = tile_y
                    
                    georef = "E{0}N{1}".format(t_min_x / TILE_SIZE, t_max_y / TILE_SIZE,)
                    georef_list.append(georef)
        else:
            georef = "E{0}N{1}".format(min_x / TILE_SIZE, max_y / TILE_SIZE,)
            georef_list.append(georef)
    
    return georef_list

def tiles_in_geom(shapely_geometry_32651):
    pow2_bounds = get_pow2_extents(get_bounds_1x1km(shapely_geometry_32651.bounds), TILE_SIZE)
    rootrect = list(pow2_bounds)
    rootnode = Node(None, rootrect)
    tree = QuadTree(rootnode, TILE_SIZE, shapely_geometry_32651)
    tile_list = []
    for leaf in tree.get_leaf_nodes():
        leaf_tile_size = get_tile_size(leaf.rect)
        min_x, min_y, max_x, max_y = leaf.rect
        if leaf_tile_size > TILE_SIZE:
            for tile_y in xrange(min_y + TILE_SIZE,
                         max_y + TILE_SIZE,
                         TILE_SIZE):
                for tile_x in xrange(min_x,
                                     max_x,
                                     TILE_SIZE):
                    
                    t_min_x = tile_x
                    t_max_x = tile_x +TILE_SIZE
                    t_min_y = tile_y -TILE_SIZE
                    t_max_y = tile_y
                    
                    tile = extents_to_bbox([t_min_x,
                                            t_min_y,
                                            t_max_x,
                                            t_max_y])
                    tile_list.append(tile)
        else:
            tile = extents_to_bbox([min_x,
                                    min_y,
                                    max_x,
                                    max_y])
            tile_list.append(tile)
    
    return tile_list

if __name__=="__main__":
    print "georefmapper.mapper"
        

