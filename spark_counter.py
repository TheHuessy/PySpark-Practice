import os
from pyspark import SparkContext
import pathlib
#import sys
import argparse

def get_args():
    args_setup = argparse.ArgmuentParser()
    args_setup.add_argument("--file_uri", help="The uri pointing to the file or folder of files you wish to count")
    return(args.parse_args())


print(get_args())


## Accept file uri as parameter
## This will need to be a SparkUtils class that has file count as a method
    ## Or a module and figure out how to pass command line arg into Spark
        ## Should be as easy as getting ARGV in the code
## Read in file via uri (hadoop or not)
    ## Will that require too much file handling?

#print(sys.argv)


#sc = SparkContext(master='yarn', appName="R-Log-Test")


#data = sc.textFile("{}/user/pi/pyspark_practice/2020-11-01.csv".format(os.environ['NAMENODE_PATH']))

