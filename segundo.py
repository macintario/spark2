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

def contarPalabras(documrnto):
    lista = []
    for palabra in set(documento):
        lista.append((palabra,documento.count(palabra)))
    return lista

def cortar(stringDoc):
    return stringDoc.split()

def wordcountbyline():
    rddSplitted = lines.map(cortar)
    wcXline = rddSplitted.map(contarPalabras)
    for i in rddSplitted.collect():
        print(i)


try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    lines = sc.textFile("noticias100.csv")
    print("rdd noticias----------------------------------")
    #print(lines)
    print("-----------------------------------------------")

    rddLineasSplitted = lines.map(lambda x:x.split()).map(x: wordcountbyline)


    for linea in rddLineasSplitted.collect():
        print(linea)


    #for linea in lines.collect():
    #    print(linea)
    rddFlatMapLineas = lines.flatMap(lambda x: x.split())
    #print(rddFlatMapLineas.collect())
    rddTuplasLlaveValor = rddFlatMapLineas.map(lambda x: (x,1))
    rddReducer = rddTuplasLlaveValor.reduceByKey(lambda x, y: x+y)
    #for palabra in rddReducer.collect():
    #    print(palabra)

    rddMapLineas = lines.map(lambda x: x.split()).flatMap(lambda x: (x, 1))

    #for linea in rddMapLineas.collect():
    #    print(linea)
    #print(rddMapLineas.collect())
    #for linea in rddMapLineas.collect():
    #    print(linea)
    rddWordCountByGroup = lines.flatMap(lambda x: x.split()) \
                                .map(lambda x:(x, 1)) \
                                .groupByKey() \
                                .mapValues(list)  \
                                .mapValues(sumar)
#    for palabra in rddWordCountByGroup.collect():
#        print(palabra)

    print("success")

except ImportError as e:
    print ("error importing spark modules", e)
    sys.exit(1)