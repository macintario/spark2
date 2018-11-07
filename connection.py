import os
import sys
import findspark as fs
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_191-amd64"
os.environ["SPARK_HOME"] = "/git/sprk"
os.environ["PYTHONPATH"] = "%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.7-src.zip:%PYTHONPATH%"
PYSPARK_PYTHON
sys.path.append("/git/sprk/python")
sys.path.append("/git/sprk/python/lib/py4j-0.10.7-src.zip")
sys.path.append("/git/sprk/python/lib")


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