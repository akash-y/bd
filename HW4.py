from pyspark import SparkContext 
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


def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''
    import csv
    import pyproj
    import shapely.geometry as geom
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('boroughs.geojson')   
    index1, zones1 = createIndex('neighborhoods.geojson')   

    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        #pdt = row[0][5:7]
        #if pdt !='02': continue # skip all records != around 10:xx pickup
        p = geom.Point(proj(float(row[3]), float(row[2])))
        
        match = None
        
        n_zone = zones.boro_name[findZone(p, index, zones)]#, zones1.geometry[findZone(q, index1, zones1)]

        if n_zone:
            counts[n_zone] = counts.get(n_zone, 0) + 1
    return counts.items()

if __name__ = 'main':
    sc = SparkContext()
    rdd = sc.textFile('yellow.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips) \
            .reduceByKey(lambda x,y: x+y) \
            .collect()
    print(counts)