import findspark as fs
fs.init()

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

def cosine_similarity(v1, v2):
    "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
    sumxx, sumxy, sumyy = 0., 0., 0.
    for i in range(len(v2)):
        x = v1[i] + 0.
        y = v2[i] + 0.
        sumxx += x * x
        sumyy += y * y
        sumxy += x * y
    return sumxy / math.sqrt(sumxx * sumyy)


conf = SparkConf()
sc = SparkContext(conf=conf)

documentos = sc.textFile("hdfs://node1/sparkhomework/noticias100.csv")

rrdMinusculas = documentos.map(lambda documento: documento.lower())

rddDocsTokenized = rrdMinusculas.map(word_tokenize)

##### Quitar Stop Words ######

stopWords = sc.textFile("hdfs://node1/sparkhomework/stopwords.txt")

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

rddConsulta = sc.parallelize(["suecia europa mundo spark"])

rddVectorConslta = rddConsulta.map(lambda consulta: crearVectorConsulta(consulta,terminos))


laConsulta = list(rddVectorConslta.collect())

rddDistancias = rrdMatrizTerminoDoc.map(lambda vectorTD: cosine_similarity(vectorTD,laConsulta))

for i in rddDistancias.collect():
    print (i)

print("############################")

#takeOrdered

