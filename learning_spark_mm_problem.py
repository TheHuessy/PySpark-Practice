import sys
from pyspark.sql import SparkSession
from pysaprk.sql.functions import count

if __name__ == "__main__":

    ## Check that we've supplied the file path to the csv
    ## Kill the program if it isn't supplied

    if len(sys.argv) != 2:
        print("Usage: learning_spark_mm_problem.py [filepath]", file=sys.stderr)
        sys.exit(-1)

    ## Start spark SQL session
    spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())

    ##Assign file path to a variable
    mnm_file_path = sys.argv[1]

    ## Load the csv into a dataframe
    mnm_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnm_file_path)

    ## Create dataframe of aggregated counts by color and state
    count_mnm_df = mnm_df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)

    ## Call the counts dataframe to execute the transformations and show the first 60 lines
    count_mnm_df.show(n=60, truncate=False)

    ## Print the resulting totals
    print("Total Rows: {count_mnm_df.count()}")



    #### FILTERING DATA TO JUST SHOW CALIFRONIA ####

    ## Create secondary dataframe with just CA data
    ca_count_mnm_df = mnm_df.select("State", "Color", "Count").where(mnm_df.State == "CA").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)

    ## Call CA dataframe and execute transformations
    ca_count_mnm_df.show(n=10, truncate=False)

    ## End spark session
    spark.stop()

