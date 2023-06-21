"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re as r


def ingest_data():
    with open("clusters_report.txt",'r') as arch:
        report = arch.readlines()

    headers=[]                              # columns=["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"]
    head2=[1,2]                             # posiciones donde el header necesita 2 lineas
    contenido=False
    clusters=[]                             # donde se guardaran de manera temporal todos los clusters
    clust=[]                                # donde se guardara de manera temporal 1 cluster a la vez

    for a in report:                        # se recorre linea por linea...
        if not contenido:                   # si no es contenido, debe ser header
            #           HEADER              #
            if r.match("^\w",a):            # si comienza texto, primer encabezado
                headers= [i.strip().replace(" ","_") for i in a.lower().split("  ") if i not in ('',"\n")][:-1]

            elif r.match("^\s{9}\w",a):     # si no es el primer encabezado r.match("\w",a):
                aux=[i.strip() for i in a.lower().strip().split("  ") ]  # if i not in ('',"\n")
                for b in head2:
                    headers[b]+="_"+aux.pop(0).strip().replace(" ","_")
                contenido= True

            else:                           # esto nunca deberia pasar...
                contenido= True
                print("Cuidado, caso no apreciado")
            #           FINALIZA HEADER     #

        else:
            #           CONTENIDO           #
            if r.match("^\s{3}\d",a):       # si es una linea que contiene index de cluster...
                b= a.split()
                clust= [int(b[0]),int(b[1]),float(str(b[2]).replace(',','.')),(" ".join(b[4:])).replace("\s*"," ")]

            elif r.match("^\s{41}\w",a):    # si es solo otra linea con mucho texto...
                clust[3]+= r.sub("\s+"," ",(" "+a.strip()).replace(".",""))   #(" "+a.strip()).replace(".","")
            else:                           # si es el final del cluster...
                if not clust: continue
                clusters.append(clust)
                clust=[]
            #           FINALIZA CONTENIDO  #

    df= pd.DataFrame(clusters,columns=headers)

    return df