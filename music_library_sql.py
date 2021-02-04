import os
import pandas as pd
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from SQLUtils import SQLUtils
from subprocess import PIPE, Popen


## Set SQL engine stuff here
## Create data extraction functions and execute them in the spark context

##Just turn it into a csv here and come back to do a sql connection
    ## doesn't look like it's worth it from a compute point of view vs just generating the file
    ## Test!


## Write out function to hadoop

def pandas_to_hadoop(data_frame, file_path):
    ## Save dataframe
    data_frame.to_csv("temp.csv")

    ## Build hdfs path
    hdfs_path = os.path.join(os.sep, 'user', 'pi', file_path)

    ## Generate hdfs command
    put = Popen(["hadoop", "fs", "-put", "temp.csv", hdfs_path], stdin=PIPE, bufsize=-1)
    ## Execute it
    put.communicate()

    ## Kill the temp file
    os.remove("temp.csv")

sql_engine = SQLUtils()
music_dat = sql_engine.get(table_name='music_lib_origin')
pandas_to_hadoop(music_dat, "music_database.csv")


if __name__ == "__main__":

    ## Check that we've supplied the file path to the csv
    ## Kill the program if it isn't supplied

    if len(sys.argv) != 2:
        print("Usage: learning_spark_mm_problem.py [hdfs_filepath_from_user/pi]", file=sys.stderr)
        sys.exit(-1)
    
    file_path = sys.argv[1]
    

    ## Create/update the underlying csv from postgres
    sql_engine = SQLUtils()
    music_dat = sql_engine.get(table_name='music_lib_origin')
    pandas_to_hadoop(music_dat, file_path)


    ## Start spark SQL session
    spark = (SparkSession.builder.appName("MusicLibCheck").getOrCreate())

    ##Assign file path to a variable
    file_path = sys.argv[1]

    ## Load the csv into a dataframe
    music_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
    music_df.show()



    ## Create dataframe of aggregated counts by color and state
#    count_mnm_df = mnm_df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)

    ## Call the counts dataframe to execute the transformations and show the first 60 lines
 #   count_mnm_df.show(n=60, truncate=False)
 #   count_mnm_df.show()

    ## Print the resulting totals
  #  print("Total Rows: {}".format(count_mnm_df.count()))



    #### FILTERING DATA TO JUST SHOW CALIFRONIA ####

    ## Create secondary dataframe with just CA data
   # ca_count_mnm_df = mnm_df.select("State", "Color", "Count").where(mnm_df.State == "VT").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)

    ## Call CA dataframe and execute transformations
    #ca_count_mnm_df.show(n=10, truncate=False)

    ## End spark session
    spark.stop()

