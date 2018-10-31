# -*- coding: utf-8 -*-

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


lema_d = CargarDiccionarioLemas()

lista=['comemos','corredor','caballos','semillas','niñas','estereotipos','cámaras','celulares','televisiones',
        'procesadores','cuesta','cobrar','vale']

for palabra in lista:
     print(lematizador(lema_d,palabra))
