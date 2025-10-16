"""
Script creado por Angela del Pozo
01/10/2025

Carga un conjunto de tablas de CoFFEE sobre beneficiarios y genera una tabla final agragada que se carga en SIGEFE

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

def read_CoFFEE_beneficiarios(input_dir):
    """
    Lee todos los archivos .xlsx de un directorio dado y los devuelve como un
    diccionario de DataFrames. Si el directorio no existe lanza un error,
    si no hay ficheros Excel informa de ello y los problemas puntuales a la hora
    de leer cada archivo quedan capturados.
    """
    #l_fields = ['Tipo Operación','Código único IJ','Código Actuación','Código Contrato','Denominación IJ/Operaciones','Nombre Destinatario','NIF Destinatario normalizado','Rol Destinatario','Naturaleza calculada Destinatario','Tipo Contrato','Importe Destinatarios sin IVA','Importe total Destinatarios']

    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]
    
    if not files:
        raise IOError("El directorio está vacío: %s" % (input_dir))

    hash_IJ2beneficiarios = {}
    hash_IJ2proyecto = {}

    for file in files:
        
        filepath = os.path.join(input_dir, file)

        df = pd.read_excel(filepath,header=2)

        l_col = list(df.columns)

        for i,row in df.iterrows():

            l_row = list(map(str,list(row)))
            l_row = [elem if elem != 'nan' else '' for elem in l_row]

            hash_row = dict(zip(l_col,l_row))

            tipo_op    = hash_row['Tipo Operación']
            cod_ij     = hash_row['Código único IJ']
            cod_coffee = hash_row['Código Actuación']

            hash_IJ2beneficiarios.setdefault(tipo_op,{})
            hash_IJ2proyecto.setdefault(tipo_op,{})

            hash_IJ2beneficiarios[tipo_op][cod_ij] = hash_row
            hash_IJ2proyecto[tipo_op][cod_ij] = cod_coffee
            
        
    return hash_IJ2beneficiarios,hash_IJ2proyecto

#######################################################

def read_CoFFEE_proyectos(input_file):

    #l_fields = ['Código Iniciativa','Estado Iniciativa','Denominación Iniciativa','Fecha Fin','CCAA','Provincia','Importe IJ/Operaciones sin IVA','Importe total IJ/Operaciones','Importe Destinatarios sin IVA','Importe total Destinatarios']

    df = pd.read_excel(input_file, header=2)

    hash_proyectos = {}

    l_col = list(df.columns)

    for i,row in df.iterrows():

        l_row = list(map(str,list(row)))
        l_row = [elem if elem != 'nan' else '' for elem in l_row]

        hash_row = dict(zip(l_col,list(map(str,l_row))))

        cod_coffee = hash_row['Código Iniciativa']

        hash_proyectos[cod_coffee] = hash_row
        #hash_proyectos[row[1]] = hash_row

    return hash_proyectos

#######################################################

def read_CoFFEE_operaciones(input_file):

    #l_fields = ['Código único IJ/Operaciones','Código contrato','Código operación','Código IJ/Operaciones','Código iniciativa','URL licitación','URL concesión','Aplicación presupuestaria','Código órgano contratación','Código BDNS','Denominación IJ/Operaciones','Fecha publicación','Fecha formalización','Tipo contrato','Importe IJ/Operaciones sin IVA','Importe total IJ/Operaciones','Observaciones']

    df = pd.read_excel(input_file, header=2)

    l_col = list(df.columns)
    
    hash_operaciones = {}

    for i,row in df.iterrows():

        l_row = list(map(str,list(row)))
        l_row = [elem if elem != 'nan' else '' for elem in l_row]

        hash_row = dict(zip(l_col,l_row))

        cod_ij = hash_row['Código único IJ/Operaciones']

        hash_operaciones[cod_ij] = hash_row

    return hash_operaciones

#######################################################

def get_cols_tabla_maestra():

    hash_col2fields = OrderedDict([('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIÓN','Observaciones')])
    
    return hash_col2fields

def get_cols_subvenciones():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código iniciativa'),
    ('NOMBRE ACTUACION','Denominación IJ/Operaciones'),
    ('CODIGO_BDNS','Código BDNS'),
    ('URL_BDNS','URL concesión'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_subvenciones():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Actuación'),
    ('NOMBRE ACTUACIÓN','Denominación IJ/Operaciones'),
    ('CODIGO_BDNS','Código BDNS'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Estado','Estado Iniciativa'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('OBSERVACIÓN','Observaciones')])

    return hash_col2fields

def get_cols_contratos():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código iniciativa'),
    ('NOMBRE ACTUACION','Denominación IJ/Operaciones'),
    ('TIPO CONTRATO','Tipo contrato'),
    ('COD_ORGANO (SOLO PLACSP)','Código órgano contratación'),
    ('COD_LICITACION (SOLO PLACSP)','URL licitación'),
    ('COD_CONTRATO','Código contrato'),
    ('URL_CONTRATO','URL concesión'),
    ('DENOMINACION','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_contratos():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Actuación'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('COD_CONTRATO','Código contrato'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('ES_SUBCONTRATISTA (SI/NO)','Rol Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO (ADJ)','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('ENLACE','URL concesión'),
    ('ESTADO','Estado Iniciativa'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (N=SI, S=NO)','Destino Subproyecto'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_convenios():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_CONVENIO','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('ENLACE','URL concesión'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_convenios():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Actuación'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (SI/NO)','Destino Subproyecto'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

"""
def get_cols_aportaciones_dinearias():

    hash_col2fields = OrderedDict([('CODIGO ACTUACION (PROYECTO O SUBPROYECTO)','Código Actuación'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_APORTACION_DINERARIA','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('NIF','NIF Destinatario'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('OBSERVACIONES','Observaciones'),
    ('ENLACE','URL concesión'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('CCAA','CCAA'),
    ('PROVINCIA','Provincia'),
    ('Perceptor final (SI/NO)','Preguntar si se deduce')])

    return hash_col2fields
"""

def get_cols_encargo():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_ENCARGO','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('CODIGO_BDNS','Código BDNS'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_encargo():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Actuación'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (SI/NO)','Destino Subproyecto'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

#######################################################

def crea_tabla_maestra(hash_col2fields,hash_IJ2beneficiario,hash_IJ2proyecto,hash_proyectos):

    df = pd.DataFrame(columns=list(hash_col2fields.keys()))

    hash_NIF = {}

    for id_ij in hash_IJ2beneficiario.keys():

        hash_benf = hash_IJ2beneficiario[id_ij]

        nif = hash_benf['NIF Destinatario normalizado']

        id_act = hash_IJ2proyecto[id_ij]

        hash_proy = hash_proyectos.get(id_act,{})

        if nif in hash_NIF:
            continue

        hash_NIF[nif] = True

        l_row = []

        for col in hash_col2fields.keys():

            field = hash_col2fields[col]

            if field in hash_benf:
                l_row.append(hash_benf[field])
            elif field in hash_proy:
                l_row.append(hash_proy[field])
            else:
                l_row.append('')

        df.loc[len(df)] = l_row

    return df.sort_values(by=['NIF'])

def crea_tabla_beneficiarios_IJ(hash_col2fields,hash_IJ2beneficiario,hash_IJ2proyecto,hash_proyectos,**kwargs):

    hash_bdns = {}

    if 'BDNS' in kwargs:
        hash_bdns = kwargs['BDNS']
    
    df = pd.DataFrame(columns=list(hash_col2fields.keys()))

    for id_ij in hash_IJ2beneficiario.keys():

        hash_benf = hash_IJ2beneficiario[id_ij]
        
        id_act = hash_IJ2proyecto[id_ij]

        hash_proy = hash_proyectos.get(id_act,{})

        l_row = []

        for col in hash_col2fields.keys():

            if col == "CODIGO_BDNS" and hash_bdns != {}:
                l_row.append(hash_bdns.get(id_ij,""))
                continue

            field = hash_col2fields[col]
            
            if field in hash_benf:
                l_row.append(hash_benf[field])
            elif field in hash_proy:
                l_row.append(hash_proy[field])
            else:
                l_row.append('')
   
        df.loc[len(df)] = l_row
    
    return df.sort_values(by=['CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)'])

def crea_tabla_IJ(hash_col2fields,hash_IJ2proyecto,hash_IJ2operaciones,hash_proyectos):
    
    df = pd.DataFrame(columns=list(hash_col2fields.keys()))

    for id_ij in hash_IJ2proyecto.keys():

        hash_oper = hash_IJ2operaciones[id_ij]

        id_act = hash_IJ2proyecto[id_ij]

        hash_proy = hash_proyectos.get(id_act,{})

        l_row = []

        for col in hash_col2fields.keys():

            field = hash_col2fields[col]

            if field in hash_oper:
                l_row.append(hash_oper[field])
            elif field in hash_proy:
                l_row.append(hash_proy[field])
            else:
                l_row.append('')
   
        df.loc[len(df)] = l_row
    
    return df.sort_values(by=['CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)'])

#######################################################

def obtiene_BDNS(hash_IJ2operaciones):

    hash_bdns = {}

    for id_ij in hash_IJ2operaciones.keys():
        hash_bdns[id_ij] = hash_IJ2operaciones[id_ij]['Código BDNS']
    
    return hash_bdns

#######################################################

def main(logger):
    
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
    
    logger.info("Leyendo tablas de entrada de CoFFEE")

    hash_IJ2beneficiario,hash_IJ2proyecto = read_CoFFEE_beneficiarios(input_dir)
    
    hash_proyectos = read_CoFFEE_proyectos(input_proyectos)

    hash_IJ2operaciones = read_CoFFEE_operaciones(input_operaciones)

    hash_BDNS = obtiene_BDNS(hash_IJ2operaciones)

    logger.info("Lectura finalizada")

    hash_df = {}

    hash_IJ2beneficiario_flat = {}
    hash_IJ2proyecto_flat = {}

    for cod_op in hash_IJ2beneficiario.keys():
        hash_IJ2beneficiario_flat.update(hash_IJ2beneficiario[cod_op])
        hash_IJ2proyecto_flat.update(hash_IJ2proyecto[cod_op])

    #hash_df['TABLA MAESTRA']               = crea_tabla_maestra(get_cols_tabla_maestra(),hash_IJ2beneficiario_flat,hash_IJ2proyecto_flat,hash_proyectos)
    #hash_df['SUBVENCIONES']                = crea_tabla_IJ(get_cols_subvenciones(),hash_IJ2proyecto['Subvención'],hash_IJ2operaciones,hash_proyectos)
    #hash_df['BENEFICIARIOS_SUBVENCIONES']  = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_subvenciones(),hash_IJ2beneficiario['Subvención'],hash_IJ2proyecto['Subvención'],hash_proyectos,BDNS=hash_BDNS)
    #hash_df['CONTRATOS']                   = crea_tabla_IJ(get_cols_contratos(),hash_IJ2proyecto['Contrato'],hash_IJ2operaciones,hash_proyectos)
    #hash_df['BENEFICIARIOS_CONTRATOS']     = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_contratos(),hash_IJ2beneficiario['Contrato'],hash_IJ2proyecto['Contrato'],hash_proyectos)
    #hash_df['CONVENIOS']                   = crea_tabla_IJ(get_cols_convenios(),hash_IJ2proyecto['Convenio'],hash_IJ2operaciones,hash_proyectos)
    #hash_df['BENEFICIARIOS_CONVENIOS']     = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_convenios(),hash_IJ2beneficiario['Convenio'],hash_IJ2proyecto['Convenio'],hash_proyectos)
    #hash_df['ENCARGOS']                    = crea_tabla_IJ(get_cols_encargo(),hash_IJ2proyecto['Encargo a medio propio'],hash_IJ2operaciones,hash_proyectos)
    #hash_df['BENEFICIARIOS_ENCARGOS']      = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_encargo(),hash_IJ2beneficiario['Encargo a medio propio'],hash_IJ2proyecto['Encargo a medio propio'],hash_proyectos)
    hash_df['OTROS'] = crea_tabla_IJ(get_cols_encargo(),hash_IJ2proyecto['Otros – Especificar'],hash_IJ2operaciones,hash_proyectos)
    hash_df['BENEFICIARIOS_OTROS']      = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_encargo(),hash_IJ2beneficiario['Otros – Especificar'],hash_IJ2proyecto['Otros – Especificar'],hash_proyectos)
    
    #hash_df['APORTACIONES_DINERARIAS']    = crea_tabla(get_cols_aportaciones_dinearias(),hash_IJ2beneficiario[''],hash_IJ2proyecto[''],hash_operaciones,hash_proyectos)

    with pd.ExcelWriter(output_file) as writer:  
        for nombre_hoja in hash_df.keys():
            hash_df[nombre_hoja].to_excel(writer, sheet_name=nombre_hoja, index=False)
    
    logger.info("Escrito fichero de salida: %s" % (output_file))

    
if __name__ == '__main__':

    try:
        logger = logging.getLogger(__name__)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',  stream=sys.stdout, encoding='utf-8')

        main(logger)

    except Exception as e:
        
        logger.error("Error general en la ejecución: %s" % (e))

        print(f"Error general en la ejecución: {e}")
        

