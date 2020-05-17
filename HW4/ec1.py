from pyspark import SparkContext
import sys 
import csv

def createIndex(shapefile):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None


def filterWords(records):
    
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')  
            
    with open('drug_sched2.txt') as f:
        list1 = f.read().splitlines() 
    
    with open('drug_illegal.txt') as f:
        list2 = f.read().splitlines()
    
    word_list = list1+list2
    
    
    counts = {}
    
    for record in records:

        try:
            fields = record.split('|')
            words = fields[6].split(' ')
                    
            
            if bool(set(words) & set(word_list)):
                lat = fields[1]
                lon = fields[2]
        
                
                p = geom.Point(proj(float(lon), float(lat)))
                zone = findZone(p, index, zones)
                
                if zone:
                
                    tract_id = zones.loc[0][0]
                    pop = zones.loc[0][1]
            
                    zone_pop = tract_id,pop
                    print("zone",zone)

                    counts[zone_pop] = counts.get(zone_pop, 0) + 1.0
                    
                    
        except:
            continue
            
    print(counts)
    return counts.items()

def toCSVLine(data):
    return ','.join(str(d) for d in data)
                
                
if __name__== "__main__":

    sc=SparkContext()

    sys_output = sys.argv[1]

    weet = sc.textFile('hdfs:///tmp/bdm/tweets-100m.csv')      
    matchedtweet = weet.mapPartitions(filterWords) \
                    .reduceByKey(lambda x,y: x+y) \
                    .map(lambda x: (x[0][0],(x[1]/x[0][1])))  \
                    .sortByKey() \
                    .map(toCSVLine) \
                    .saveAsTextFile(sys_output)
 
