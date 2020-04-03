from pyspark import SparkContext
import dateutil
import csv

if __name__=='__main__':
    sc = SparkContext()
    data = sc.textFile('/tmp/bdm/complaints.csv') 
    header = data.first() 
    data = data.filter(lambda x: x != header) \
        .mapPartitions(lambda x: csv.reader(x, delimiter=',')) \
        .map(lambda x: ((x[1],dateutil.parser.parse(x[0]).year,x[7]),1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: ((x[0][0], x[0][1]),(x[0][2],x[1]))) \
        .reduceByKey(lambda x,y: x + y) \
        .map(lambda x: (x[0][0], x[0][1], sum(i for i in x[1][1::2]),len(x[1][::2]),
                        (100*max(i for i in x[1][1::2])/sum(i for i in x[1][1::2])))) \
        .collect()
