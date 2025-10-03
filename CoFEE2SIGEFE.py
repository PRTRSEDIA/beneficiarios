"""
Script creado por Angela del Pozo
01/10/2025

Carga un conjunto de tablas de CoFFEE sobre beneficiarios y genera una tabla final agragada que se carga en SIGEFE

"""

import os
import pandas as pd
import argparse

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def read_CoFFEE_beneficiarios(input_dir):
    """
    Lee todos los archivos .xlsx de un directorio dado y los devuelve como un
    diccionario de DataFrames. Si el directorio no existe lanza un error,
    si no hay ficheros Excel informa de ello y los problemas puntuales a la hora
    de leer cada archivo quedan capturados.
    """
    l_fields = ['Código Actuación','Nombre Destinatario','NIF Destinatario normalizado','Tipo documento','Tipo Operación','Naturaleza calculada Destinatario','Importe Destinatarios sin IVA','Importe total Destinatarios']

    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]
    
    if not files:
        raise IOError("El directorio está vacío: %s" % (input_dir))

    hash_IJ2beneficiarios = {}
    hash_IJ2proyecto = {}

    for file in files:
        
        filepath = os.path.join(input_dir, file)

        df = pd.read_excel(filepath,header=2)

        for i,row in df[['Código único IJ']+l_fields].iterrows():
            hash_IJ2beneficiarios[row[0]] = list(map(str,list(row[1:])))
            hash_IJ2proyecto[row[0]] = row[1]
            
        
    return l_fields,hash_IJ2beneficiarios,hash_IJ2proyecto

##########

def read_CoFFEE_proyectos(input_file):

    l_fields = ['Órgano Gestor','CCAA','Provincia']

    df = pd.read_excel(input_file, header=2)

    hash_proyectos = {}

    for i,row in df[['Código Iniciativa','Código provisional iniciativa']+l_fields].iterrows():
        hash_proyectos[row[0]] = list(map(str,list(row[2:])))
        hash_proyectos[row[1]] = list(map(str,list(row[2:])))

    return l_fields,hash_proyectos

##########

def read_CoFFEE_operaciones(input_file):

    l_fields = ['Código iniciativa','Tipo actuación','URL licitación']

    df = pd.read_excel(input_file, header=2)
    
    hash_operaciones = {}

    for i,row in df[['Código único IJ/Operaciones']+l_fields].iterrows():
        hash_operaciones[row[0]]= list(map(str,list(row[1:])))

    return l_fields,hash_operaciones

##########

def main():
    
    parser = argparse.ArgumentParser(description='Formateo de la información de beneficiarios para la carga en SIGEFE')

    parser.add_argument('--input', '-i', type=str, default=None, help='Ruta de la carpeta donde buscar archivos .xlsx de beneficiarios CoFFEE de entrada')
    parser.add_argument('--output', '-o', type=str, default=None, help='Ruta y nombre del fichero de salida .xlsx con la tabla agregada')
    parser.add_argument('--proyectos', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de proyectos')
    parser.add_argument('--operaciones', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de operaciones')
        
    args = parser.parse_args()

    input_dir = args.input

    if not os.path.exists(input_dir):
        raise IOError('El directorio de entrada con las hojas de beneficiarios, no existe: %s' % (input_dir))
    
    output_file = args.output

    output_dir = os.path.dirname(output_file)

    if not os.path.exists(output_dir):
        raise IOError('El directorio donde se va a escribir el excel de salida no existe: %s' % (output_dir))

    input_proyectos = args.proyectos

    if not os.path.exists(input_proyectos):
        raise IOError('El xlsx de entrada con los proyectos, no existe: %s' % (input_proyectos))
    
    input_operaciones = args.operaciones

    if not os.path.exists(input_operaciones):
        raise IOError('El xlsx de entrada con las operaciones, no existe: %s' % (input_operaciones))

    l_fields_ben,hash_IJ2beneficiario,hash_IJ2proyecto = read_CoFFEE_beneficiarios(input_dir)

    #l_fields_ben,hash_IJ2beneficiario,hash_IJ2proyecto = [],{},{}
    
    l_fields_proy,hash_proyectos = read_CoFFEE_proyectos(input_proyectos)

    l_fields_op,hash_operaciones = read_CoFFEE_operaciones(input_operaciones)

    l_header = ['Código único IJ/Operaciones']+l_fields_proy+l_fields_op+l_fields_ben
    
    df_output = pd.DataFrame(columns=l_header)

    for i,id_ij in enumerate(hash_IJ2beneficiario.keys()):

        id_act = hash_IJ2proyecto[id_ij]
               
        l_proy = hash_proyectos.get(id_act,[])

        l_operaciones = hash_operaciones.get(id_ij,[])

        beneficiario = hash_IJ2beneficiario[id_ij]
        
        df_output.loc[len(df_output)] = [id_ij] + l_proy + l_operaciones + beneficiario

        print("Leída el Instrumento Jurídico %d/%d" % (i+1,len(hash_IJ2beneficiario.keys())))

    df_output.to_excel(output_file, index=False)
    
if __name__ == '__main__':

    try:
        main()
    except Exception as e:
        print(f"Error general en la ejecución: {e}")

