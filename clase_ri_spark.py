####################################################################################
# INSTITUTO POLITECNICO NACIONAL
# CENTRO DE INVESTIGACION EN COMPUTACION
# DIPLOMADO EN DESCUBRIMIENTO DEL CONOCIMIENTO CON
# HERRAMIENTAS BIG DATA
# PRACTICA: RECUPERACION DE LA INFORMACION CON SPARK
# ALUMNO: IVAN GUTIERREZ RODRIGUEZ
####################################################################################

import os
import sys

os.environ['JAVA_HOME'] = "/usr/java/jdk1.8.0_191-amd64"
os.environ['SPARK_HOME'] = "/git/spark-2.3.2-bin-hadoop2.7"

sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python")
sys.path.append("/git/spark-2.3.2-bin-hadoop2.7/python/lib/py4j-0.10.6-src.zip")


from nltk import word_tokenize
from pyspark import SparkContext
from pyspark import SparkConf
import math as math


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
            if lematizador(lema_d, palabradocs) == palabra:
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


def crearVectorConsultaFlatmap(w,lsConsulta):
    if w in lsConsulta:
        return 1
    else:
        return 0

def cosine_similarity(v1, v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"

    sumxx, sumxy, sumyy = 0., 0., 0.
    for i in range(len(v2)):
        x = v1[i] + 0.
        y = v2[i] + 0.
        sumxx += x * x
        sumyy += y * y
        sumxy += x * y
    prod = sumxx * sumyy

    if prod >0:
        return sumxy / math.sqrt(prod)
    else:
        return 0

conf = SparkConf()
sc = SparkContext(conf=conf)

documentos = sc.textFile("noticias100.csv")

rddIdxDoc  = documentos.zipWithIndex()

rrdMinusculas = rddIdxDoc.map(lambda (documento,idx) : (documento.lower(),idx))


rddDocsTokenized = rrdMinusculas.map(lambda (documento,idx):(word_tokenize(documento),idx))

##### Quitar Stop Words ######

stopWords = sc.textFile("stopwords.txt")

rrdStopWordsMinusculas = stopWords.map(lambda stopWord: stopWord.lower())

listaStopWords = rrdStopWordsMinusculas.collect()

##### Quitar Stop Words ###########################
print("\n*******Quitando stop words")
rddWordsInLista = rddDocsTokenized.flatMap(lambda (word,idx): word)

rddDocumentosSinSW = rddWordsInLista.filter(lambda word: word not in listaStopWords)
print("\n********Lematizando")
lema_d = CargarDiccionarioLemas()
rddWordsLematized = rddDocumentosSinSW.map(lambda word:lematizador(lema_d,word))

diccionarioDeTerminos = rddWordsLematized.distinct()
terminos = list(diccionarioDeTerminos.collect())
print("\n******** Matriz TD")
rrdMatrizTerminoDoc = rddDocsTokenized.map(lambda (documento,idx): (crearVector(documento,terminos), idx))

lsConsulta = ["amarillo", "gris", "verde", "azul", "naranja"]

print(".")
print("\n######## BUSCANDO .....")
print(lsConsulta)

rddConsulta = diccionarioDeTerminos.map(lambda w: crearVectorConsultaFlatmap(w, lsConsulta) )

lsvConsulta = rddConsulta.collect()

rddDistancias = rrdMatrizTerminoDoc.map(lambda (vectorTD, idx): (cosine_similarity(vectorTD,lsvConsulta),idx))

top = rddDistancias.takeOrdered(10,key = lambda x: -x[0])
print("###############ENCONTRADOS")


listaTopNoticias = list()
for distancia, noticia in top:
    if distancia > 0:
        print(" "+str(noticia)+"  -  " + str(distancia))
        listaTopNoticias.append(noticia)

rddTextoNoticias = rddIdxDoc.filter(lambda (txtNoticia,idx): idx in  listaTopNoticias )
textoNoticiasTop = rddTextoNoticias.collect()

print("###############RESULTADOS")
#imprimimos en orden
cntr = 1
for distancia, noticia in top:
    for i in textoNoticiasTop:
        if noticia == i[1]:
            print(str(cntr) +".- id:" + str(noticia) + "  similitud:" + str(distancia))
            print (i[0])
            cntr += 1

print("############################")
