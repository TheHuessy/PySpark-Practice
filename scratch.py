## Will need to source spark env vars prior to running

import os
from pyspark import SparkContext

## For most data transformation think of each function as being applied to a row
def clean_spark_data(row_data):
    return([clean_row.replace('"', '') for clean_row in row_data])

## Attempt at "logical" data processing
def package_freq_by_date(row):
    output_row = [row[0], row[6]]

    ##The thing to return would be a "new" row object...


## Start spark context

sc = SparkContext(master='yarn', appName="R-Log-Test")

## Pull in text data from hdfs
data = sc.textFile("{}/user/pi/pyspark_practice/2020-11-01.csv".format(os.environ['NAMENODE_PATH']))
## Split data into a series of row 'arrays'
rows = data.map(lambda x: x.split(',')).map(clean_spark_data)

date_freq = rows.map(lambda x: (x[0], 1)).countByKey()

print(date_freq)


#print(rows.take(5))
