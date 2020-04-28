from pyspark import SparkContext
import csv
import pyproj
import shapely.geometry as geom
from heapq import nlargest
import heapq
import sys


def createIndex(shapefile):
    
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None


def processTrips(pid, records):
    
    import csv
    import pyproj
    import shapely.geometry as geom
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index1, zones1 = createIndex('boroughs.geojson')   
    index2, zones2 = createIndex('neighborhoods.geojson')   

    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    output={}
    
    for row in reader:
        
        try:
            p = geom.Point(proj(float(row[5]), float(row[6]))) 
            q = geom.Point(proj(float(row[9]), float(row[10])))
            match1 = None
            match2 = None
            b_zone = findZone(p, index1, zones1)
            n_zone = findZone(q, index2, zones2)
            b_val = zones1['boroname']['b_zone']
            n_val = zones2['neighborhood']['n_zone']
            nb_zone = b_val,n_val
            
            if nb_zone:
                counts[nb_zone] = counts.get(nb_zone, 0) + 1 
        
        except(ValueError, IndexError):
            pass
                
          
 
    return counts.items()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

if __name__== "__main__":

    sc=SparkContext()

    sys_input = sys.argv[1]
    sys_output = sys.argv[2]
    
    rdd = sc.textFile(sys_input)
    rdd.mapPartitionsWithIndex(processTrips) \
        .reduceByKey(lambda x,y: x+y) \
        .filter(lambda x: x[0][0] != None) \
        .filter(lambda x: x[0][1] != None) \
        .map(lambda x: (x[0][0],x[0][1],x[1]))  \
        .groupBy(lambda x:x[0]) \
        .flatMap(lambda g: nlargest(3,g[1],key=lambda x:x[2])) \
        .map(lambda x:(x[0],(x[1],x[2]))) \
        .groupByKey().mapValues(list) \
        .reduceByKey(lambda x,y: x+y) \
        .sortByKey() \
        .map(toCSVLine) \
        .saveAsTextFile(sys_output)
