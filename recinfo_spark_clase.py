import os
import sys

os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip")


from pyspark import SparkContext
from pyspark import SparkConf
from nltk import word_tokenize, ngrams



def CargarDiccionarioLemas():
    file=open("diccionarioLematizador.txt","rb")
    lema_d={}

    for line in file:
        #print(line)
        bloques = line.split()
        palabra = bloques[0]
        lema = bloques[1]
        #print("i",a,b)
        #print( bloques[0],bloques[1])
        lema_d.update({palabra:lema})
    return lema_d

def lematizador(lema_d,palabra):
    palabra=palabra.lower()
    if palabra in lema_d:
        lema = str(lema_d.get(palabra))
    else:
        lema = palabra
    return lema

def crearVector(documento,diccionario):
    vector = []

    for palabra in diccionario:
        contador = 0
        for palabradocs in documento:
            if lematizador(lema_d,palabradocs) == palabra:
                contador = contador + 1
            vector.append(contador)
    return vector


def crearVectorConsulta(consulta,diccionario):
    vector = []

    for palabra in diccionario:
        contador = 0
        for palabradocs in consulta.split():
            if lematizador(lema_d,palabradocs) == palabra:
                contador = contador + 1
                vector.append(contador)

    return vector



conf = SparkConf()
sc = SparkContext(conf=conf)

documentos = sc.textFile("noticias100.csv")

rrdMinusculas = documentos.map(lambda documento: documento.lower())

rddDocsTokenized = rrdMinusculas.map(word_tokenize)

##### Quitar Stop Words ######

stopWords = sc.textFile("stopwords.txt")

rrdStopWordsMinusculas = stopWords.map(lambda stopWord: stopWord.lower())

listaStopWords = rrdStopWordsMinusculas.collect()

##### Quitar Stop Words ###########################

rddWordsInLista = rddDocsTokenized.flatMap(lambda word: word)

rddDocumentosSinSW = rddWordsInLista.filter(lambda word: word not in listaStopWords)

##### Quitar Stop Words ###########################
#lematizador(lema_d,palabra)
lema_d = CargarDiccionarioLemas()
rddWordsLematized = rddDocumentosSinSW.map(lambda word:lematizador(lema_d,word))

diccionarioDeTerminos = rddWordsLematized.distinct()
terminos = list(diccionarioDeTerminos.collect())



rrdMatrizTerminoDoc = rddDocsTokenized.map(lambda documento: crearVector(documento,terminos))

rddConsulta = sc.parallelize(["hola mundo de spark"])

rddVectorConslta = rddConsulta.map(lambda consulta: crearVectorConsulta(consulta,terminos))


for i in rrdMatrizTerminoDoc.collect():
    print (i)

print("############################")
print(rddVectorConslta.collect())