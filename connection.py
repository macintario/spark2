import os
import sys

os.environ['SPARK_HOME'] = "/home/aulae1-b6/spark"

sys.path.append("/home/aulae1-b6/spark/python")
sys.path.append("/home/aulae1-b6/spark/python/lib/py4j-0.10.6-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    r = sc.parallelize([1,2,3,4,5])
    print(r)
    print(r.collect())
    print(r.take(2))
    print(r.count())
    print("success")

except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)