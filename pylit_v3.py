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
            
        try:
            if row[23]:
                if row[23].isalpha():
                    continue
            
                split = row[23].replace('-',' ').split()
                combined = [int(val) for val in split]
            
            
                house_number =combined[-1]
            
                if len(combined)==2:    
                    split_val = combined[0]
                else:
                    split_val = 0
            
                if house_number%2==0:
                    odd_even='even'
                else:
                    odd_even='odd'
            else:
                continue

        
        except:
            continue
            
    
        
        yield house_number,split_val,Y2015,Y2016,Y2017,Y2018,Y2019,boro,street_name,odd_even
       
    
def streetprocess(pid,records):
    
    if pid ==0:
        next(records)
        
    reader = csv.reader(records)
    
    for row in reader:
        
        physical_id = int(row[0])
        street_name = str(row[28].upper())
        boro_code = int(row[13])
        st_name = str(row[29].upper())
                
        try:  
            if row[2]:
               
                if row[2].isalpha():
                    continue
                    
                ll_split = row[2].replace('-',' ').split()
                ll_combined = [int(val) for val in ll_split]
                    
                ll_hno = ll_combined[-1]
                    
                if len(ll_combined)==2:    
                    ll_split_val = ll_combined[0]
                else:
                    ll_split_val = 0
            
                if ll_hno%2==0:
                    odd_even='even'
                else:
                    odd_even='odd'
            else:
                continue
                
            if row[3]:
                    
                if row[3].isalpha():
                    continue
                    
                lh_split = row[3].replace('-',' ').split()
                lh_combined = [int(val) for val in lh_split]
                    
                lh_hno = lh_combined[-1]
                    
                if len(lh_combined)==2:    
                    lh_split_val = lh_combined[0]
                else:
                    lh_split_val = 0
                    
            else:
                continue
                
            if row[4]:
                    
                if row[4].isalpha():
                    continue
                    
                rl_split = row[4].replace('-',' ').split()
                rl_combined = [int(val) for val in rl_split]
                    
                rl_hno = rl_combined[-1]
                    
                if len(rl_combined)==2:    
                    rl_split_val = rl_combined[0]
                else:
                    rl_split_val = 0
            
                if rl_hno%2==0:
                    odd_even='even'
                else:
                    odd_even='odd'
                    
            else:
                continue
                        
            if row[5]:
                    
                if row[5].isalpha():
                    continue
                    
                rh_split = row[5].replace('-',' ').split()
                rh_combined = [int(val) for val in rh_split]
                    
                rh_hno = rh_combined[-1]
                    
                if len(rh_combined)==2:    
                    rh_split_val = rh_combined[0]
                else:
                    rh_split_val = 0
            
                if ll_hno%2==0:
                    odd_even='even'
                else:
                    odd_even='odd'
                    
            else:
                continue
                        
                        
        except:
            continue
            
        yield physical_id,ll_hno,lh_hno,ll_split_val,lh_split_val,street_name,boro_code,odd_even
        yield physical_id,ll_hno,lh_hno,ll_split_val,lh_split_val,st_name,boro_code,odd_even                
        yield physical_id,rl_hno,rh_hno,rl_split_val,rh_split_val,street_name,boro_code,odd_even                
        yield physical_id,rl_hno,rh_hno,rl_split_val,rh_split_val,st_name,boro_code,odd_even 
        
        
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

    violations_df = spark.createDataFrame(counts, schema=('house_number','split_val','Y2015','Y2016','Y2017','Y2018','Y2019','boro','street_name','odd_even')).dropDuplicates()

    violations_df.cache()
    
    centerline_df = spark.createDataFrame(counts2,schema=('physical_id','l_house_number','h_house_number','l_split_val','h_split_val','street_name','boro_code','odd_even')).dropDuplicates().sort('physical_id')

    centerline_df.cache()
    
    boro_condition = (violations_df.boro == centerline_df.boro_code)

    street_condition = (violations_df.street_name == centerline_df.street_name)

    odd_even_condition = (violations_df.odd_even == centerline_df.odd_even)

    house_condition = (violations_df.split_val >= centerline_df.l_split_val)&(violations_df.split_val <= centerline_df.h_split_val)&(violations_df.house_number >= centerline_df.l_house_number)&(violations_df.house_number <= centerline_df.h_house_number)

    final_df = violations_df.join(f.broadcast(centerline_df),[boro_condition,street_condition,odd_even_condition,house_condition],how='inner').groupby(centerline_df.physical_id).agg(sum(violations_df.Y2015).alias('COUNT_2015'),sum(violations_df.Y2016).alias('COUNT_2016'),sum(violations_df.Y2017).alias('COUNT_2017'),sum(violations_df.Y2018).alias('COUNT_2018'),sum(violations_df.Y2019).alias('COUNT_2019')).sort("physical_id")
    
    final_df.cache()

    final_df = final_df.fillna(0)

    final_df = final_df.withColumn('OLS_COEFF', lit(calculate_slope_udf(final_df['COUNT_2015'],final_df['COUNT_2016'],final_df['COUNT_2017'],final_df['COUNT_2018'],final_df['COUNT_2019'])))

    #final_df = final_df.select('PHYSICALID',col('sum(2015)').alias('COUNT_2015'),col('sum(2016)').alias('COUNT_2016'),col('sum(2017)').alias('COUNT_2017'),col('sum(2018)').alias('COUNT_2018'),col('sum(2019)').alias('COUNT_2019'),'OLS_COEFF')
    
    
    final_df.show(1000)
    
    print('time taken:', time.time() - start_time)
