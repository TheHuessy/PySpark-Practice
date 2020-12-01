## Will need to source spark env vars prior to running

import os
import sys
from pyspark import SparkContext
from pyspark import SQLContext

## For most data transformation think of each function as being applied to a row
#def prepare_raw_spark_data(row_data):
#    clean_rows = [clean_row.replace('"', '') for clean_row in row_data]
#    print(clean_rows)
#    print(dir(clean_rows))
#    return(clean_rows)
#    return([clean_row.replace('"', '') for clean_row in row_data])

## Start spark context
sc = SparkContext(master='yarn', appName="R-Log-Test")

## Start spark SQL context
sqlsc = SQLContext(sc)

## Pull in text data from hdfs
rows = sc.textFile("{}/user/pi/pyspark_practice/2020-11-01.csv".format(os.environ['NAMENODE_PATH'])).map(lambda x: x.replace('"', '')).map(lambda x: x.split(','))

## Create the dataframe from the cleaned RDD object
full_df = sqlsc.createDataFrame(data = rows.filter(lambda x: x[0] != 'date'),
        schema=rows.filter(lambda x: x[0] == 'date').collect()[0])

## Cache the df in RAM for easier transformation and calling
full_df.persist()

#Have a look:
#sql_df.show(10)

full_df.groupBy("package").count().sort("count", ascending = False).show(10)


