from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
import numpy as np
import sys
import time
import csv

start_time = time.time()

def ticketprocess(pid,records):
    
    import dateutil.parser 
    
    
    boro_dict = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1,
                'BRONX':2,'BX':2,
                'BK':3,'K':3,'KING':3,'KINGS':3,
                'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,
                'R':5,'RICHMOND':5}
    
    if pid == 0:
        next(records)
    reader = csv.reader(records)
                    
    for row in reader:

        try:
            if row[23]:
                
                if row[23].isdigit():
                    
                    house_number = int(row[23])
            
                    if house_number%2==0:
                        odd_even = 'even'
                
                    else:
                        odd_even = 'odd'
            
                elif row[23].isalpha():
                    continue
                    
                
                else:
                    
                    
                    split_val = int(row[23].replace('-', ' ').split(' ')[0])
                    hno = int(row[23].replace('-', ' ').split(' ')[1])
                    house_number = int(str(split_val)+str(hno))
                    
                    odd_even = None
              
                    
                    if int(house_number)%2==0:
                        odd_even = 'even'
                
                    else:
                        odd_even = 'odd'
                    
            else:
                continue
        
        except(ValueError, IndexError, KeyError):
            pass
        
        try:
        
            street_name = str(row[24].upper())
            year = str(dateutil.parser.parse(row[4]).year)
            
            Y2015 = 0
            Y2016 = 0
            Y2017 = 0
            Y2018 = 0
            Y2019 = 0
    
            
            if year == '2015':
                Y2015 = 1
            elif year == '2016':
                Y2016 = 1
            elif year == '2017':
                Y2017 = 1
            elif year == '2018':
                Y2018 = 1
            elif year == '2019':
                Y2019 = 1
            else:
                continue
                
 
        
            if row[21] in boro_dict.keys():
                boro = int(boro_dict[row[21]])
        
        except:
            continue
        
        yield house_number,year,Y2015,Y2016,Y2017,Y2018,Y2019,boro,street_name,odd_even 
        
        
#Writing function to retrieve left and right low and high house numbers, street_name, borocode and physicalID

def streetprocess(pid,records):
            
    if pid ==0:
        next(records)
        
    reader = csv.reader(records)
    
    for row in reader:
        
        try:
             
            if row[2]:
                
                if row[2].isdigit():
                    
                    ll_house_number = int(row[2])
                    
                elif row[2].isalpha():
                        continue
                    
                else:
                    ll_split_val = int(row[2].replace('-', ' ').split(' ')[0])
                    ll_hno = int(row[2].replace('-', ' ').split(' ')[1])
                    ll_house_number = int(str(ll_split_val)+str(ll_hno))
                                        
            else:
                continue
                
                
            if row[3]:
                if row[3].isdigit():
                    
                    lh_house_number = int(row[3])

                
                elif row[3].isalpha():
                        continue
                    
                else:
                    lh_split_val = int(row[3].replace('-', ' ').split(' ')[0])
                    lh_hno = int(row[3].replace('-', ' ').split(' ')[1])
                    lh_house_number = int(str(lh_split_val)+str(lh_hno))
                    
            else:
                continue
                
            if row[4]:
                if row[4].isdigit():
                    
                    rl_house_number = int(row[4])
                    
                
                elif row[4].isalpha():
                        continue
                    
                else:
                    rl_split_val = int(row[4].replace('-', ' ').split(' ')[0])
                    rl_hno = int(row[4].replace('-', ' ').split(' ')[1])
                    rl_house_number = int(str(rl_split_val)+str(rl_hno))
                                        
            else:
                continue
                
                
            if row[5]:
                if row[5].isdigit():
                    
                    rh_house_number = int(row[5])

                
                elif row[5].isalpha():
                        continue
                    
                else:
                    rh_split_val = int(row[5].replace('-', ' ').split(' ')[0])
                    rh_hno = int(row[5].replace('-', ' ').split(' ')[1])
                    rh_house_number = int(str(rh_split_val)+str(rh_hno))
                    
            else:
                continue
                
        except(ValueError, IndexError, KeyError):
            pass
        
        physical_id = int(row[0])
        street_name = str(row[28].upper())
        boro_code = int(row[13])
        st_name = str(row[29].upper())
        
        
        
        yield physical_id,ll_house_number,lh_house_number,street_name,boro_code,'odd'
        yield physical_id,ll_house_number,lh_house_number,st_name,boro_code,'odd'
        yield physical_id,rl_house_number,rh_house_number,street_name,boro_code,'even'
        yield physical_id,rl_house_number,rh_house_number,st_name,boro_code,'even'
        
        
def func_slope(v1,v2,v3,v4,v5):
    
    try:
        X = np.array([2015,2016,2017,2018,2019])
        y = np.array([v1,v2,v3,v4,v5])
    
        m = (((np.mean(X)*np.mean(y)) - np.mean(X*y)) / ((np.mean(X)**2) - np.mean(X*X)))
    
        return float(m)
    except:
        return 0.0

calculate_slope_udf = udf(func_slope, FloatType())



if __name__ == "__main__":

    sc = SparkContext()
    spark = SparkSession(sc)
    
    #sys_output = sys.argv[1]

    rdd = sc.textFile('hdfs:///tmp/bdm/nyc_parking_violation/')
    rdd2 = sc.textFile('hdfs:///tmp/bdm/nyc_cscl.csv')
    counts = rdd.mapPartitionsWithIndex(ticketprocess)
    counts2 = rdd2.mapPartitionsWithIndex(streetprocess)

    violations_df = spark.createDataFrame(counts, schema=('house_number','year','Y2015','Y2016','Y2017','Y2018','Y2019','boro','street_name','odd_even')).dropDuplicates().dropna(how='any)
    centerline_df = spark.createDataFrame(counts2,schema=('physical_id','low_house_number','high_house_number','street_name','boro_code','odd_even')).dropDuplicates().dropna(how='any).sort('physical_id')

    final_df = violations_df.join(f.broadcast(centerline_df),
                              [centerline_df.street_name == violations_df.street_name,
                              centerline_df.boro_code == violations_df.boro,
                              centerline_df.odd_even == violations_df.odd_even,
                             (violations_df.house_number >= centerline_df.low_house_number) & 
                              (violations_df.house_number <= centerline_df.high_house_number)], 
                              how ='right').groupby(centerline_df.physical_id).agg(sum(violations_df.Y2015).alias('COUNT_2015'),sum(violations_df.Y2016).alias('COUNT_2016'),sum(violations_df.Y2017).alias('COUNT_2017'),sum(violations_df.Y2018).alias('COUNT_2018'),sum(violations_df.Y2019).alias('COUNT_2019')).sort("physical_id")

    final_df = final_df.fillna(0)

    final_df = final_df.withColumn('OLS_COEFF', lit(calculate_slope_udf(final_df['COUNT_2015'],final_df['COUNT_2016'],final_df['COUNT_2017'],final_df['COUNT_2018'],final_df['COUNT_2019'])))


    final_df.show(10000)
    print('time taken:', time.time() - start_time)
