# quadtree.py
# Implements a Node and QuadTree class that can be used as 
# base classes for more sophisticated implementations.
# Malcolm Kesson Dec 19 2012
import time
from shapely.geometry.geo import shape
from shapely.geometry.polygon import Polygon
from automation.index.utils import is_square, write_tile_to_shape, TILE_SIZE

    
class Node():
    ROOT = 0
    BRANCH = 1
    LEAF = 2
    minsize = 1   # Set by QuadTree
    #_______________________________________________________
    # In the case of a root node "parent" will be None. The
    # "rect" lists the minx,minz,maxx,maxz of the rectangle
    # represented by the node.
    def __init__(self, parent, rect):
        self.parent = parent
        self.children = [None,None,None,None]
        if parent == None:
            self.depth = 0
        else:
            self.depth = parent.depth + 1
        self.rect = rect
        #print "NODE RECT: "+str(rect)+" DEPTH: "+str(self.depth)
        x0,z0,x1,z1 = rect
        if self.parent == None:
            self.type = Node.ROOT
        elif (x1 - x0) <= Node.minsize:
            self.mark_as_leaf()
        else:
            self.type = Node.BRANCH
    
    def mark_as_leaf(self):
        self.type = Node.LEAF
    #_______________________________________________________
    # Recursively subdivides a rectangle. Division occurs 
    # ONLY if the rectangle spans a "feature of interest".
    def subdivide(self, ref_geom):
        if self.type == Node.LEAF:
            return
        x0,z0,x1,z1 = self.rect
        h = (x1 - x0)/2
        
        # SPACE FILLING CURVE TRAVERSAL?
        rects = []
        #ul_quad = 
        #dl_quad
        #dr_quad
        #ur_quad
        rects.append( (x0, z0, x0 + h, z0 + h) )
        rects.append( (x0, z0 + h, x0 + h, z1) )
        rects.append( (x0 + h, z0 + h, x1, z1) )
        rects.append( (x0 + h, z0, x1, z0 + h) )
        for n in range(len(rects)):
            #Spanning check
            #~ span = self.spans_feature(rects[n], ref_shp)
            #~ print "DEPTH: "+str(self.depth) + " N-SON: "+str(n) + " SPAN: " + str(span)
            #~ if span == True:
                #~ #Creates node
                #~ self.children[n] = self.getinstance(rects[n])
                #~ #Subdivides node
                #~ self.children[n].subdivide(ref_shp) # << recursion
            
            #
            poly_int = self.get_intersection_geometry(rects[n], ref_geom)
            if poly_int:
                self.children[n] = self.getinstance(rects[n])
                if not is_square(poly_int):
                    self.children[n].subdivide(ref_geom) # << recursion
                else:   #Mark full squares as leaves
                    self.children[n].mark_as_leaf()
            
    #_______________________________________________________
    # A utility proc that returns True if the coordinates of
    # a point are within the bounding box of the node.
    def contains(self, x, z):
        x0,z0,x1,z1 = self.rect
        if x >= x0 and x <= x1 and z >= z0 and z <= z1:
            return True
        return False
    #_______________________________________________________
    # Sub-classes must override these two methods.
    def getinstance(self,rect):
        return Node(self, rect)      
        
    def spans_feature(self, rect, ref_shape):
        min_x, min_y, max_x, max_y = rect
        tile_ulp = (min_x, max_y)
        tile_dlp = (min_x, min_y)
        tile_drp = (max_x, min_y)
        tile_urp = (max_x, max_y)
        tile = Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])
        #return not tile.intersection(geom).is_empty
        for feature in ref_shape:
            shp_geom = shape(feature['geometry'])
            poly_int =tile.intersection(shp_geom)
            if not poly_int.is_empty:
                return True
        return False
    
    def get_intersection_geometry(self, rect, ref_geom):
        min_x, min_y, max_x, max_y = rect
        tile_ulp = (min_x, max_y)
        tile_dlp = (min_x, min_y)
        tile_drp = (max_x, min_y)
        tile_urp = (max_x, max_y)
        tile = Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])
        
        poly_int = tile.intersection(ref_geom)
        if not poly_int.is_empty:
            return poly_int

#===========================================================            
class QuadTree():
    maxdepth = 1 # the "depth" of the tree
    
    leaves = []
    allnodes = []
    #_______________________________________________________
    def __init__(self, rootnode, minrect, ref_geom, out_shp_file=None):
        #print "MINRECT: " + str(minrect)
        QuadTree.maxdepth = 1 # the "depth" of the tree
        QuadTree.leaves = []
        QuadTree.allnodes = []
        
        Node.minsize = minrect
        self.shp_file = out_shp_file
        
        #Timer
        start_time = time.time()
        
        rootnode.subdivide(ref_geom) # constructs the network of nodes
        self.prune(rootnode)
        
        end_time = time.time()
        self.elapsed_time=round(end_time - start_time,2)
        print '\nElapsed Time:', str("{0:.2f}".format(self.elapsed_time)), 'seconds'
        
        #~ schema = {
            #~ 'geometry': 'Polygon',
            #~ #'properties': dict([(u'GRID_REF', 'tr:254'), (u'MINX', 'float:19'), (u'MINY', 'float:19'), (u'MAXX', 'float:19'), (u'MAXY', 'float:19'), (u'Tilename', 'str:254'), (u'File_Path', 'str:254')])
            #~ 'properties': dict([('GRID_REF', 'str:254'), ('TYPE', 'int:1'),('MINX', 'float:19'), ('MINY', 'float:19'), ('MAXX', 'float:19'), ('MAXY', 'float:19')])
            #~ }
        #~ if out_shp_file is not None:
            #~ with fiona.open(out_shp_file, 'w','ESRI Shapefile', schema, crs=from_epsg(32651), ) as out_shp:
                #~ self.traverse(rootnode, out_shp)
        #~ else:
        self.traverse(rootnode)
    
    def get_leaf_nodes(self):
        return list(self.leaves)
    #_______________________________________________________
    # Sets children of 'node' to None if they do not have any
    # LEAF nodes.       
    def prune(self, node):
        if node.type == Node.LEAF:
            return 1
        leafcount = 0
        removals = []
        for child in node.children:
            if child != None:
                leafcount += self.prune(child)
                if leafcount == 0:
                    removals.append(child)
        for item in removals:
            n = node.children.index(item)
            node.children[n] = None     
        return leafcount
    #_______________________________________________________
    # Appends all nodes to a "generic" list, but only LEAF 
    # nodes are appended to the list of leaves.
    def traverse(self, node, shp_handler=None):
        if shp_handler is not None:
            #print "Writing {0}".format(node.rect)
            #if node.type == Node.LEAF:
            write_tile_to_shape(node.rect, shp_handler, TILE_SIZE, node.type) 
        QuadTree.allnodes.append(node)
        if node.type == Node.LEAF:
            QuadTree.leaves.append(node)
            if node.depth > QuadTree.maxdepth:
                QuadTree.maxdepth = node.depth
        for child in node.children:
            if child != None:
                self.traverse(child, shp_handler) # << recursion

    

#_______________________________________________________
# Returns a string containing the rib statement for a
# four sided polygon positioned at height "y".
def RiPolygon(rect, y): 
    x0,z0,x1,z1 = rect
    verts = []
    verts.append(' %1.3f %1.3f %1.3f' % (x0,y,z0))
    verts.append(' %1.3f %1.3f %1.3f' % (x0,y,z1))
    verts.append(' %1.3f %1.3f %1.3f' % (x1,y,z1))
    verts.append(' %1.3f %1.3f %1.3f' % (x1,y,z0))
    rib =  '\tPolygon "P" ['
    rib += ''.join(verts)
    rib += ']\n'
    return rib    
