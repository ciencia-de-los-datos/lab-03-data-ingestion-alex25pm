"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():

    #
    # Inserte su código aquí
    #

    # Leer el archivo con pd.read_fwf()
    df = pd.read_fwf("clusters_report.txt", 
                     colspecs="infer", 
                     widths=[9, 16, 16, 80], 
                     header=None, 
                     names=["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"])

    # Eliminar las filas de encabezado y completar los valores faltantes hacia adelante
    df = df.drop(index={0, 1, 2}).ffill()

    df["porcentaje_de_palabras_clave"] = df["porcentaje_de_palabras_clave"].str.rstrip(" %").str.replace(",", ".").astype(float)
    
    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()

    # Reemplazar espacios por guiones bajos en los nombres de las columnas
    df.columns = df.columns.str.replace(' ', '_')

    # astype(), está pasando un diccionario donde las claves son los nombres de las columnas
    # y los valores son los nuevos tipos de datos para las columnas.
    df = df.astype  ({ "cluster": int, 
                    "cantidad_de_palabras_clave": int, 
                    "porcentaje_de_palabras_clave": float,
                    "principales_palabras_clave": str
                    })

    # Agrupa por la tres columnas y principales palabra clave la convierte en una cadena separada por espacio
    df = df.groupby(["cluster","cantidad_de_palabras_clave","porcentaje_de_palabras_clave"])["principales_palabras_clave"].apply(lambda x: ' '.join(map(str,x))).reset_index()
    
    # la columna "porcentaje_de_palabras_clave" redondea valores a 1 decimal.
    df["porcentaje_de_palabras_clave"] = df["porcentaje_de_palabras_clave"].apply(lambda x: round(x,1))
    
    # elimina los espacios en blanco adicionales y los puntos al final de cada cadena en la columna "principales_palabras_clave"
    df["principales_palabras_clave"] = df["principales_palabras_clave"].apply(lambda x: re.sub(r'\s+', ' ',x).rstrip("."))
  
    return df

