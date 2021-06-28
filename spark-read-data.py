# Run using spark-submit spark-read-datat.py

import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Read Text to RDD - Python")
  sc = SparkContext(conf=conf)
  # read input text file to RDD
  lines = sc.textFile("hdfs://namenodeserver:8020/tmp/sentry_poc/poc_inputfile1.txt")
  # collect the RDD to a list
  llist = lines.collect()
  # print the list
  for line in llist:
    print(line)
