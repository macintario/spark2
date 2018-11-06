import os
import sys

os.environ['SPARK_HOME'] = "/home/aulae1-b6/spark"

sys.path.append("/home/aulae1-b6/spark/python")
sys.path.append("/home/aulae1-b6/spark/python/lib/py4j-0.10.6-src.zip")



try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from nltk import word_tokenize, ngrams
    lema_d=dict()

    def CargarDiccionarioLemas():
        rddLemas = sc.textFile("diccionarioLematizador.txt")
        # lema_d={}
        file = rddLemas.collect()
        for line in file:
            # print(line)
            bloques = line.split()
            palabra = bloques[0]
            lema = bloques[1]
            # print("i",a,b)
            # print( bloques[0],bloques[1])
            lema_d.update({palabra: lema})


        return lema_d


    def lematizador(lema_d, palabra):
        palabra = palabra.lower()
        if palabra in lema_d:
            lema = str(lema_d.get(palabra))
        else:
            lema = palabra
        return lema


    def cosine_similarity(v1, v2):
        "compute cosine similarity of v1 to v2: (v1 dot v2)/{||v1||*||v2||)"
        sumxx, sumxy, sumyy = 0., 0., 0.
        for i in range(len(v1)):
            x = v1[i] + 0.;
            y = v2[i] + 0.
            sumxx += x * x
            sumyy += y * y
            sumxy += x * y


        return sumxy / math.sqrt(sumxx * sumyy)

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    documento = sc.textFile("noticias100.csv")
    rddMinusculas = documento.map(lambda documento: documento.lower())
    rddWordsTokenized = rddMinusculas.map(word_tokenize)
    ##stop words
    stopwords = sc.textFile("stopwords.txt")
    rddStopWordMinusculas = stopwords.map(lambda  stopword: stopword.lower())
    listaStopWords = rddStopWordMinusculas.collect()
    rddWordsinLista = rddWordsTokenized.flatMap(lambda word: word)

    rddDocumentosSinSw = rddWordsinLista.filter(lambda word: word not in listaStopWords)

    #lematizador
    lema_d = CargarDiccionarioLemas()
    rddLematizada = rddDocumentosSinSw.map(lambda word: lematizador(lema_d,word))
    rddDiccionarioDeTerminos = rddLematizada.distinct()
    vector = []
    def creaVector(documento,diccionario):
        for palabra in diccionario:
            contador = 0
            for palabradocs in documento:
                if lematizador(lema_d,palabradocs) == palabra:
                    contador += 1
            vector.append(contador)
        return vector

    def creaVectorConsulta(consulta,terminos):
        for palabra in diccionario:
            contador = 0
            for palabradocs in consulta.split():
                if lematizador(lema_d,palabradocs) == palabra:
                    contador += 1
            vector.append(contador)
        return vector


    terminos = list(rddDiccionarioDeTerminos.collect())
    rddMatrizTerminoDoc = rddWordsTokenized.map(lambda documento: creaVector(documento,terminos))

    rddConsulta = sc.parallelize(["hola mundo de spark"])
    rddVectorConsulta = rddConsulta.map(lambda consulta: crearVectorConsulta(consulta,terminos))
    rddDistancia = rddMatrizTerminoDoc.map(lambda x: cosine_similarity(X, rddVectorConsulta.collect()))
    rddDistancia.collect()
    #for y in rddLematizada.collect():
    #   print(y)

    print("success")

except ImportError as e:
    print("error importing spark modules", e)
    sys.exit(1)