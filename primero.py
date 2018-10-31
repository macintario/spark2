import os
import sys

os.environ['SPARK_HOME'] = "/home/aulae1-b6/spark"

sys.path.append("/home/aulae1-b6/spark/python")
sys.path.append("/home/aulae1-b6/spark/python/lib/py4j-0.10.6-src.zip")


def sumar(listaValores):
    suma = 0
    for valor in listaValores:
        suma += valor
    return suma

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    lines = sc.textFile("noticias100.csv")
    print("rdd noticias----------------------------------")
    print(lines)
    print("-----------------------------------------------")
    #for linea in lines.collect():
     #   print(linea)
    rddFlatMapLineas = lines.flatMap(lambda x: x.split())
    print(rddFlatMapLineas.collect())
    rddTuplasLlaveValor = rddFlatMapLineas.map(lambda x: (x,1))
    rddReducer = rddTuplasLlaveValor.reduceByKey(lambda x, y: x+y)
    for palabra in rddReducer.collect():
        print(palabra)

    #for linea in rddMapLineas.collect():
    #    print(linea)
    rddMapLineas = lines.map(lambda x: x.split())
    #print(rddMapLineas.collect())
    #for linea in rddMapLineas.collect():
    #    print(linea)
    rddWordCountByGroup = lines.flatMap(lambda x: x.split()) \
                                .map(lambda x:(x, 1)) \
                                .groupByKey() \
                                .mapValues(list)  \
                                .mapValues(sumar)
    for palabra in rddWordCountByGroup.collect():
        print(palabra)

    print("success")

except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)