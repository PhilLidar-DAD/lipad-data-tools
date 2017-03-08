import sys, ast
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
sys.path.append('/home/ken/thesis_workspace/automation')
from automation import georef_mapper
from pprint import pprint

def are_lists_equal(L1, L2):
    return len(L1) == len(L2) and sorted(L1) == sorted(L2)

def are_polys_equal(P1, P2):
    return P1.area == P2.area and P1.bounds == P2.bounds and P1.length == P2.length and P1.geom_type == P2.geom_type and P1.almost_equals(P2)

def test_extents_to_georef_1():
    extents=[243102.123,1678117.197,244102.387,1679117.187,]
    georef="E243N1679"
    assert georef_mapper.extents_to_georef(extents) == georef 
    
def test_extents_to_georef_2():
    extents=[233332.0,1677232.0,256423.0,1714523,]
    georef="E233N1714"
    assert georef_mapper.extents_to_georef(extents) == georef 

def test_georef_to_extents_1():
    extents=[243000,1678000,244000,1679000,]
    georef="E243N1679"
    result_extents=georef_mapper.georef_to_extents(georef)
    assert are_lists_equal(result_extents, extents) == True

def test_georef_to_extents_2():
    extents=[233000,1713000,234000,1714000,]
    georef="E233N1714"
    result_extents=georef_mapper.georef_to_extents(georef)
    assert are_lists_equal(result_extents, extents) == True
    
def test_extents_to_bbox():
    extents = [233000,1713000,234000,1714000,]
    tile_bbox = Polygon([[233000, 1714000], [233000, 1713000], [234000, 1713000], [234000, 1714000]])
    assert are_polys_equal(georef_mapper.extents_to_bbox(extents), tile_bbox)
    
def test_georefs_in_geom():
    test_georefs=[]
    feature_mp = None
    with open("test_poly_pts.txt") as pts_src, open("test_poly_georefs.txt") as georefs_src:
        poly_list = []
        for georef_line in georefs_src:
            test_georefs.append(georef_line.strip()) 
        for poly_line in pts_src:
            pts_list = list(ast.literal_eval(poly_line.strip()))
            poly = Polygon(pts_list)
            poly_list.append(poly)
        feature_mp = MultiPolygon(poly_list)
        result_georefs = georef_mapper.georefs_in_geom(feature_mp)
    assert are_lists_equal(test_georefs, result_georefs) == True

def test_is_georef_in_geom_1():
    #Tests all georefs that are inside the geometry
    with open("test_poly_pts.txt") as pts_src, open("test_poly_georefs.txt") as georefs_src:
        poly_list = []
        for poly_line in pts_src:
            pts_list = list(ast.literal_eval(poly_line.strip()))
            poly = Polygon(pts_list)
            poly_list.append(poly)
        feature_mp = MultiPolygon(poly_list)
        for georef_line in georefs_src:
            assert georef_mapper.is_georef_in_geom(georef_line.strip(), feature_mp) is True 

def test_is_georef_in_geom_2():
    #Tests all georefs that are outside the geometry
    pass
        
if __name__=="__main__":
    test_georefs=[]
    feature_mp = None
    with open("test_poly_pts.txt") as pts_src, open("test_poly_georefs.txt") as georefs_src:
        poly_list = []
        for georef_line in georefs_src:
            test_georefs.append(georef_line.strip()) 
        for poly_line in pts_src:
            pts_list = list(ast.literal_eval(poly_line.strip()))
            poly = Polygon(pts_list)
            poly_list.append(poly)
        feature_mp = MultiPolygon(poly_list)
        result_georefs = georef_mapper.georefs_in_geom(feature_mp)
        #pprint(result_georefs)
        #pprint(test_georefs)
        #pprint(set(result_georefs).symmetric_difference(set(test_georefs)))
        print "LENGTHS: [{0} | {1}]".format(len(result_georefs), len(test_georefs))
        pprint(sorted(test_georefs) == sorted(result_georefs))
        pprint(set(result_georefs) ^ set(test_georefs))