
"""
Script creado por Angela del Pozo
14/10/2025

Carga un conjunto de tablas de BDNS, las parsea y busca si están todos los ID de COFFEE

"""

import os, sys
import pandas as pd
import argparse

from collections import OrderedDict

import datetime
import random
import string

import warnings
import logging

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


#######################################################

def read_tablas_BDNS(input_dir):
    
    l_fields = ['Código','Título / Descripción','Nacionalidad','NIF/CIF','Nombre / Razón Social','Código de concesión','Instrumento de Ayuda (Descripción)','Fecha de la concesión','Coste actividad']

    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]
    
    if not files:
        raise IOError("El directorio está vacío: %s" % (input_dir))

    hash_BDNS = {}
    
    for file in files:
        
        filepath = os.path.join(input_dir, file)

        df = pd.read_excel(filepath,header=4)

        for i,row in df[l_fields].iterrows():

            l_row = list(map(str,list(row)))
            l_row = [elem if elem != 'nan' else '' for elem in l_row]

            hash_row = dict(zip(l_fields,l_row))

            hash_BDNS[hash_row['Código']]  = hash_row
            hash_BDNS[hash_row['NIF/CIF']] = hash_row

    return hash_BDNS

#######################################################

def read_CoFFEE_IJ_beneficiarios(input_dir):

    l_fields = ['Tipo Operación','Código único IJ','Código Actuación','Denominación IJ/Operaciones','Nombre Destinatario','NIF Destinatario normalizado','Rol Destinatario','Naturaleza calculada Destinatario','Importe Destinatarios sin IVA','Importe total Destinatarios']

    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]
    
    if not files:
        raise IOError("El directorio está vacío: %s" % (input_dir))

    hash_IJ2beneficiarios = {}

    for file in files:
        
        filepath = os.path.join(input_dir, file)

        df = pd.read_excel(filepath,header=2)

        for i,row in df[l_fields].iterrows():

            l_row = list(map(str,list(row)))
            l_row = [elem if elem != 'nan' else '' for elem in l_row]

            hash_row = dict(zip(l_fields,l_row))

            if hash_row['Tipo Operación'] != "Subvención":
                continue

            hash_IJ2beneficiarios[hash_row['Código único IJ']] = hash_row
        
    return hash_IJ2beneficiarios

#######################################################

def read_CoFFEE_IJ(input_file):

    #l_fields = ['Código único IJ/Operaciones','Código operación','Código IJ/Operaciones','Código iniciativa','URL licitación','URL concesión','Código BDNS','Denominación IJ/Operaciones','Fecha formalización','Importe IJ/Operaciones sin IVA','Importe total IJ/Operaciones','Observaciones']

    df = pd.read_excel(input_file, header=2)

    l_col = list(df.columns)
    
    hash_IJ = {}

    for i,row in df.iterrows():

        l_row = list(map(str,list(row)))
        l_row = [elem if elem != 'nan' else '' for elem in l_row]

        hash_row = dict(zip(l_col,l_row))

        if hash_row['Tipo actuación'] != "Subvención":
            continue

        id_ij = hash_row['Código único IJ/Operaciones']

        hash_IJ[id_ij]= hash_row

    return hash_IJ

#######################################################

def formatea(hash_X):

    return list(map(lambda x: "%s:%s" % (x,hash_X[x]), hash_X.keys()))

#######################################################

def main(logger):
    
    parser = argparse.ArgumentParser(description='Parseo de tablas de BDNS')

    parser.add_argument('--input', '-i', type=str, default=None, help='Ruta de la carpeta donde buscar archivos .xlsx de la descarga de BDNS')
    parser.add_argument('--output', '-o', type=str, default=None, help='Ruta y nombre del fichero de salida .xlsx con la tabla de IDs validados')
    parser.add_argument('--ij', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de IJ')
    parser.add_argument('--b', type=str, default=None, help='Ruta de entrada con los .xlsx de beneficiarios')
            
    args = parser.parse_args()

    input_dir = args.input

    if not os.path.exists(input_dir):
        raise IOError('El directorio de entrada con las hojas de beneficiarios, no existe: %s' % (input_dir))
       
    input_IJ = args.ij

    if not os.path.exists(input_IJ):
        raise IOError('El xlsx de entrada con las operaciones/IJ, no existe: %s' % (input_IJ))
    
    input_beneficiarios = args.b

    if not os.path.isdir(input_beneficiarios):
        raise IOError('La ruta con los xlsx de entrada con los beneficiarios no existe: %s' % (input_beneficiarios))
    
    output_file = args.output

    output_dir = os.path.dirname(output_file)

    if not os.path.exists(output_dir):
        raise IOError('El directorio donde se va a escribir el excel de salida no existe: %s' % (output_dir))
    
    logger.info("Leyendo tablas de entrada de BDNS y COFFEE")

    hash_BDNS = read_tablas_BDNS(input_dir)

    hash_IJ = read_CoFFEE_IJ(input_IJ)

    hash_beneficiarios = read_CoFFEE_IJ_beneficiarios(input_beneficiarios)

    logger.info("Hecho!")

    df = pd.DataFrame(columns=['código IJ único','Código iniciativa','código BDNS','Denominación Subvención','URL concesión','Fecha formalización','NIF destinatario','Nombre destinatario','Rol destinatario','Ámbito destinatario','Importe sin IVA','Importe total','Observaciones','BDNS NIF','BDNS cod'])

    for i,id_ij in enumerate(hash_IJ.keys()):

        logger.info("Leyendo registro %d" % (i))

        cod_BDNS_aux = hash_IJ[id_ij]['Código BDNS']

        l_cod_BDNS = [cod_BDNS_aux]

        if cod_BDNS_aux != "":
            l_cod_BDNS = cod_BDNS_aux.split(';')

        cod_coffee = hash_IJ[id_ij]['Código iniciativa']
        denom    = hash_IJ[id_ij]['Denominación IJ/Operaciones']
        url      = hash_IJ[id_ij]['URL concesión']
        fecha    = hash_IJ[id_ij]['Fecha formalización']
        obs      = hash_IJ[id_ij]['Observaciones']
                   
        nombre, nif, rol, ambito, importe_s_IVA, importe_total = (".", ".", ".", ".", ".", ".")

        if id_ij in hash_beneficiarios:
            nombre   = hash_beneficiarios[id_ij]['Nombre Destinatario']
            nif      = hash_beneficiarios[id_ij]['NIF Destinatario normalizado']
            rol      = hash_beneficiarios[id_ij]['Rol Destinatario']
            ambito   = hash_beneficiarios[id_ij]['Naturaleza calculada Destinatario']
            importe_s_IVA = hash_beneficiarios[id_ij]['Importe Destinatarios sin IVA']
            importe_total = hash_beneficiarios[id_ij]['Importe total Destinatarios']

        for cod_BDNS in l_cod_BDNS:
            hash_nif  = hash_BDNS.get(nif,{'.':'.'})
            hash_bdns = hash_BDNS.get(cod_BDNS,{'.':'.'})

            df.loc[len(df)] = [id_ij,cod_coffee,cod_BDNS,denom,url,fecha,nif,nombre,rol,ambito,importe_s_IVA,importe_total,obs,"|".join(formatea(hash_nif)),'|'.join(formatea(hash_bdns))]

    df.to_excel(output_file,sheet_name="BDNS", index=False)

    logger.info('Escrito xlsx de salida: %s' % (output_file))

#######################################################

if __name__ == '__main__':

    try:
        logger = logging.getLogger(__name__)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',  stream=sys.stdout, encoding='utf-8')

        main(logger)

    except Exception as e:
        
        logger.error("Error general en la ejecución: %s" % (e))

        print(f"Error general en la ejecución: {e}")
        