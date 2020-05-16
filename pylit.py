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
import numpy as np
import sys
import time
import csv



def translate(mapping):
    def translate_(col):
        return mapping.get(col)
    return udf(translate_, StringType())

def func_slope(v1,v2,v3,v4,v5):
    
    try:
        X = np.array([2015,2016,2017,2018,2019])
        y = np.array([v1,v2,v3,v4,v5])
    
        m = (((np.mean(X)*np.mean(y)) - np.mean(X*y)) / ((np.mean(X)**2) - np.mean(X*X)))
    
        return float(m)
    except:
        return 0.0

calculate_slope_udf = udf(func_slope, FloatType())

boro_dict = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1,
                'BRONX':2,'BX':2,
                'BK':3,'K':3,'KING':3,'KINGS':3,
                'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,
                'R':5,'RICHMOND':5}


if __name__ == "__main__":

    sc = SparkContext()
    spark = SparkSession(sc)    

    df_violations = spark.read.csv('hdfs:///tmp/bdm/nyc_parking_violation/',header=True)

    df_violations = df_violations.select("House Number","Street Name","Issue Date","Violation County").dropna(how='any')
    
    df_violations = df_violations.withColumn("Street Name",f.upper(f.col("Street Name")))

    df_violations = df_violations.withColumn("Violation County", translate(boro_dict)("Violation County"))

    df_violations = df_violations.withColumn('Issue Date',f.year(f.to_timestamp('Issue Date', 'MM/dd/yyyy')))

    df_violations = df_violations.filter(df_violations['Issue Date'].isin(['2015','2016','2017','2018','2019']))

    df_violations = df_violations.filter(df_violations['House Number'].rlike('^[0-9]+([ -][0-9]+)?$'))

    df_violations = df_violations.withColumn("House Number",regexp_replace(col("House Number"), "-", " ").alias("House Number"))

    df_violations = df_violations.withColumn("House Number",regexp_replace(f.col("House Number"), " ", "").alias("House Number"))

    df_violations = df_violations.withColumn('House Number', f.expr('transform(House Number, x-> int(x))'))

    df_violations = df_violations.withColumn("Odd_Even",f.when((f.col("House Number")%2==0),"Even").otherwise("Odd"))

    df_violations = df_violations.groupby(df_violations.columns).count()

    df_violations = df_violations.groupby('House Number', 'Street Name', 'Violation County','Odd_Even').pivot('Issue Date', [2015,2016,2017,2018,2019]).agg(f.max('count'))
                                         
    df_violations.cache()



    df_centerline = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv',header=True)

    df_centerline_l = df_centerline.select("PHYSICALID","L_LOW_HN","L_HIGH_HN","ST_NAME","FULL_STREE","BOROCODE")
    
    df_centerline_r = df_centerline.select("PHYSICALID","R_LOW_HN","R_HIGH_HN","ST_NAME","FULL_STREE","BOROCODE")


    df_centerline_l = df_centerline_l.filter(df_centerline_l['L_LOW_HN'].rlike('^[0-9]+([ -][0-9]+)?$'))

    df_centerline_l = df_centerline_l.filter(df_centerline_l['L_HIGH_HN'].rlike('^[0-9]+([ -][0-9]+)?$'))

    df_centerline_r = df_centerline_r.filter(df_centerline_r['R_LOW_HN'].rlike('^[0-9]+([ -][0-9]+)?$'))

    df_centerline_r = df_centerline_r.filter(df_centerline_r['R_HIGH_HN'].rlike('^[0-9]+([ -][0-9]+)?$'))


    df_centerline_l = df_centerline_l.withColumn("L_LOW_HN",regexp_replace(regexp_replace(col("L_LOW_HN"), "-", " ")," ","").alias("L_LOW_HN"))

    df_centerline_l = df_centerline_l.withColumn("L_HIGH_HN",regexp_replace(regexp_replace(col("L_HIGH_HN"), "-", " ")," ","").alias("L_HIGH_HN"))

    df_centerline_r = df_centerline_r.withColumn("R_HIGH_HN",regexp_replace(regexp_replace(col("R_HIGH_HN"), "-", " ")," ","").alias("R_HIGH_HN"))

    df_centerline_r = df_centerline_r.withColumn("R_LOW_HN",regexp_replace(regexp_replace(col("R_LOW_HN"), "-", " ")," ","").alias("R_LOW_HN"))



    df_centerline_l=df_centerline_l.withColumn('Odd_Even', lit("Odd"))

    df_centerline_r=df_centerline_r.withColumn('Odd_Even', lit("Even"))

                                         
    df_centerline = df_centerline_l.union(df_centerline_r)



    df_centerline = df_centerline.select(col('PHYSICALID'),col('L_LOW_HN').alias('LOW_HN'),col('L_HIGH_HN').alias('HIGH_HN'),'ST_NAME','FULL_STREE','BOROCODE','Odd_Even')

    df_centerline = df_centerline.withColumn("PHYSICALID", df_centerline['PHYSICALID'].cast("int"))

    df_centerline.cache()

    final_df = df_violations.join(f.broadcast(df_centerline),
                             [(df_centerline['ST_NAME'] == df_violations['Street Name']) | (df_centerline['FULL_STREE'] == df_violations['Street Name']),
                              df_centerline['BOROCODE'] == df_violations['Violation County'], 
                              df_violations['Odd_Even'] == df_centerline['Odd_Even'],
                              (df_violations['House Number'] >= df_centerline['LOW_HN'])&(df_violations['House Number'] <= df_centerline['HIGH_HN'])
                             ],how = 'right').sort('PHYSICALID').fillna(0)

    columns_to_drop = ['Issue Date']

    final_df = final_df.drop(*columns_to_drop)


    final_df = final_df.withColumn('OLS_COEFF', lit(calculate_slope_udf(final_df['2015'],final_df['2016'],final_df['2017'],final_df['2018'],final_df['2019'])))

    final_df = final_df.select('PHYSICALID',col('2015').alias('COUNT_2015'),col('2016').alias('COUNT_2016'),col('2017').alias('COUNT_2017'),col('2018').alias('COUNT_2018'),col('2019').alias('COUNT_2019'),'OLS_COEFF')

    final_df.show(3000)

    print('elapsed seconds:', time.time() - start_time)
