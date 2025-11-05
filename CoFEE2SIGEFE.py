"""
Script creado por Angela del Pozo
01/10/2025

Carga un conjunto de tablas de CoFFEE sobre beneficiarios y genera una tabla final agragada que se carga en SIGEFE

"""

import os, sys
import pandas as pd
import argparse

from collections import OrderedDict

from functools import reduce
from itertools import zip_longest

import datetime
import random
import string

import warnings
import logging

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

#######################################################

def read_CoFFEE_beneficiarios(input_dir,hash_id2provisional,l_proyectos_target):
    """
    Lee todos los archivos .xlsx de un directorio dado y los devuelve como un
    diccionario de DataFrames. Si el directorio no existe lanza un error,
    si no hay ficheros Excel informa de ello y los problemas puntuales a la hora
    de leer cada archivo quedan capturados.
    """
    
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.xlsx')]
    
    if not files:
        raise IOError("El directorio está vacío: %s" % (input_dir))

    hash_IJ2beneficiarios_aux = {}
    
    for file in files:
        
        filepath = os.path.join(input_dir, file)

        df = pd.read_excel(filepath,header=2)

        l_col = list(df.columns)

        for _,row in df.iterrows():

            l_row = list(map(str,list(row)))
            l_row = [elem if elem != 'nan' else '' for elem in l_row]

            hash_benf = dict(zip(l_col,l_row))

            tipo_op    = hash_benf['Tipo Operación']
            cod_ij     = hash_benf['Código único IJ']
            cod_coffee = hash_benf['Código Actuación']
            cod_coffee_prov = hash_id2provisional.get(cod_coffee,cod_coffee)

            if filtra_proyecto(cod_coffee_prov,l_proyectos_target):
                continue

            hash_IJ2beneficiarios_aux.setdefault(tipo_op,{})
            hash_IJ2beneficiarios_aux[tipo_op].setdefault(cod_ij,[]).append(tuple(sorted(hash_benf.items())))

    hash_IJ2beneficiarios = {}

    for tipo_op in hash_IJ2beneficiarios_aux.keys():

        hash_IJ2beneficiarios.setdefault(tipo_op,{})

        for cod_ij in hash_IJ2beneficiarios_aux[tipo_op].keys():

            l_tuplas = list(set(hash_IJ2beneficiarios_aux[tipo_op][cod_ij]))

            hash_IJ2beneficiarios[tipo_op][cod_ij] = [dict(tupla) for tupla in l_tuplas]
        
    return hash_IJ2beneficiarios

#######################################################

def read_CoFFEE_proyectos(input_file,l_proyectos_target):

    df = pd.read_excel(input_file, header=2)

    hash_proyectos = {}
    hash_id2provisional = {}

    l_col = list(df.columns)

    for i,row in df.iterrows():

        l_row = list(map(str,list(row)))
        l_row = [elem if elem != 'nan' else '' for elem in l_row]

        hash_row = dict(zip(l_col,list(map(str,l_row))))

        cod_coffee      = hash_row['Código Iniciativa']
        cod_coffee_prov = hash_row['Código provisional iniciativa']
        
        if filtra_proyecto(cod_coffee_prov,l_proyectos_target):
            continue

        hash_id2provisional[cod_coffee] = cod_coffee_prov
        hash_proyectos[cod_coffee] = hash_row

    return hash_id2provisional,hash_proyectos

#######################################################

def read_CoFFEE_IJ(input_file,hash_id2provisional,l_proyectos_target):

    df = pd.read_excel(input_file, header=2)

    l_col = list(df.columns)
    
    hash_operaciones = {}

    for _,row in df.iterrows():

        l_row = list(map(str,list(row)))
        l_row = [elem if elem != 'nan' else '' for elem in l_row]

        hash_row = dict(zip(l_col,l_row))

        cod_ij     = hash_row['Código único IJ/Operaciones']
        cod_coffee = hash_row['Código iniciativa']
        cod_coffee_prov = hash_id2provisional.get(cod_coffee,cod_coffee)

        if filtra_proyecto(cod_coffee_prov,l_proyectos_target):
            continue

        tipo_op = hash_row['Tipo Operación']

        hash_operaciones.setdefault(tipo_op,{})
        
        hash_operaciones[tipo_op][cod_ij] = hash_row
        
    return hash_operaciones

#######################################################

def obtiene_aportaciones_dinerarias(hash_IJ2beneficiarios,hash_IJ2operaciones):

    hash_IJ2beneficiarios_AD = {}
    hash_IJ2operaciones_AD   = {}
    
    hash_id_ij = {}

    tipo_op = "Modificaciones de créditos"

    for id_ij in hash_IJ2beneficiarios[tipo_op].keys():

        l_hash_benf = hash_IJ2beneficiarios[tipo_op][id_ij]

        if id_ij in hash_IJ2operaciones[tipo_op]:
            hash_IJ2operaciones_AD[id_ij] = hash_IJ2operaciones[tipo_op][id_ij]

        for hash_benf in l_hash_benf:

            if hash_benf['Profundidad iniciativa'] != '3': ## sólo se selecciona nivel proyectos
                continue

            hash_id_ij[id_ij] = True
            
            hash_IJ2beneficiarios_AD.setdefault(id_ij,[]).append(hash_benf)
    
    tipo_op = "Otros – Especificar"

    for id_ij in hash_IJ2beneficiarios[tipo_op].keys():
        
        if id_ij not in hash_IJ2operaciones:
            continue

        hash_oper = hash_IJ2operaciones[id_ij]

        str_descrip = hash_oper['Denominación IJ/Operaciones']

        if str_descrip.lower().find('dineraria')!=-1 or str_descrip.lower().find('aportación')!=-1:

            hash_IJ2beneficiarios_AD[id_ij] = hash_IJ2beneficiarios[tipo_op][id_ij]

            if id_ij in hash_IJ2operaciones[tipo_op]:
                hash_IJ2operaciones_AD[id_ij] = hash_IJ2operaciones[tipo_op][id_ij]
            
            hash_id_ij[id_ij] = True
                
    return hash_IJ2beneficiarios_AD, hash_IJ2operaciones_AD

#######################################################

def formatea_numero(valor):

    if valor == '.':
        return '.'

    valor_n = float(valor)

    s = format(valor_n, ",.2f")
    
    return s.replace(".", "v").replace(',','.').replace('v',',')

def formatea_perceptor_final(valor):

    valor_n = ""

    if valor == "N":
        valor_n = "SI"
    elif valor == "S":
        valor_n = "NO"

    return valor_n

def formatea_subcontratista(valor):

    valor_n = ""

    if valor == "Contratista adjudicatario":
        valor_n = "NO"
    elif valor == "Subcontratista":
        valor_n = "SI"

    return valor_n

#######################################################

def hace_match(cof_coffee_query,cof_coffee_ref):

    l_query = cof_coffee_query.split('.')
    l_ref   = cof_coffee_ref.split('.')

    is_match = True

    for (a,b) in list(zip_longest(l_ref,l_query,fillvalue=None)):

        if a == None:
            break
        elif a != b:
            is_match = False
            break
        elif a == None and b != None:
            break

    return is_match

def filtra_proyecto(id_act,l_proyectos_target):

    componente = id_act.split('.')[0]

    if componente == "C15":
        return True
    
    if l_proyectos_target != []:

        if True in list(map(lambda id_act_ref: hace_match(id_act,id_act_ref), l_proyectos_target)):
            return False
        
        return True
    
    return False

#######################################################

def get_cols_tabla_maestra():

    hash_col2fields = OrderedDict([('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIÓN','Observaciones')])
    
    return hash_col2fields

def get_cols_subvenciones():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación IJ/Operaciones'),
    ('CODIGO_BDNS','Código BDNS'),
    ('URL_BDNS','URL concesión'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_subvenciones():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
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
    ('Perceptor final (SI/NO)','Destino Subproyecto'), # requiere regla
    ('OBSERVACIÓN','Observaciones')])

    return hash_col2fields

def get_cols_contratos():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación IJ/Operaciones'),
    ('TIPO CONTRATO','Tipo contrato'),
    ('COD_ORGANO (SOLO PLACSP)','Código órgano contratación'),
    ('COD_LICITACION (SOLO PLACSP)','URL licitación'),
    ('COD_CONTRATO','Código contrato'),
    ('DENOMINACION','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_contratos():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('COD_CONTRATO','Código IJ/Operaciones'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('ES_SUBCONTRATISTA (SI/NO)','Rol Destinatario'), # requiere regla
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO (ADJ)','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('ENLACE','URL concesión'),
    ('ESTADO','Estado Iniciativa'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'), 
    ('Perceptor final (SI/NO)','Destino Subproyecto'), # requiere regla
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_convenios():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_CONVENIO','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_convenios():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('COD_CONVENIO','Código IJ/Operaciones'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (SI/NO)','Destino Subproyecto'), # requiere regla
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_encargo():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_ENCARGO','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_encargo():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_ENCARGO','Código IJ/Operaciones'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (SI/NO)','Destino Subproyecto'), # requiere regla
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_aportaciones_dinerarias():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_APORTACION_DINERARIA','Código IJ/Operaciones'),
    ('DENOMINACIÓN','Denominación IJ/Operaciones'),
    ('FECHA_FORMALIZACION','Fecha formalización'),
    ('IMPORTE_SIN_IVA','Importe IJ/Operaciones sin IVA'),
    ('IMPORTE_INTEGRO','Importe total IJ/Operaciones'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

def get_cols_beneficiarios_aportaciones_dinerarias():

    hash_col2fields = OrderedDict([('CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)','Código Iniciativa'),
    ('NOMBRE ACTUACION','Denominación Iniciativa'),
    ('CODIGO_APORTACION_DINERARIA','Código IJ/Operaciones'),
    ('NIF','NIF Destinatario normalizado'),
    ('BENEFICIARIO','Nombre Destinatario'),
    ('IMPORTE_SIN_IVA','Importe Destinatarios sin IVA'),
    ('IMPORTE_INTEGRO','Importe total Destinatarios'),
    ('PROVINCIA','Provincia'),
    ('CCAA','CCAA'),
    ('Clase de Beneficiario (Privado/Publico)','Naturaleza calculada Destinatario'),
    ('Perceptor final (SI/NO)','Destino Subproyecto'), # requiere regla
    ('OBSERVACIONES','Observaciones')])

    return hash_col2fields

#######################################################

def crea_tabla_maestra(l_ij_target,hash_IJ2beneficiarios,hash_IJ2operaciones,hash_proyectos):

    l_cols = ['NIF','BENEFICIARIO','PROVINCIA','CCAA','OBSERVACIÓN']

    df = pd.DataFrame(columns=l_cols)

    hash_NIF = {}

    hash_IJ2beneficiarios_flat = {}
    
    for cod_op in hash_IJ2beneficiarios.keys():
        for id_ij in hash_IJ2beneficiarios[cod_op].keys():
            hash_IJ2beneficiarios_flat[id_ij] = hash_IJ2beneficiarios[cod_op][id_ij]

    hash_IJ2operaciones_flat = {}

    for cod_op in hash_IJ2operaciones.keys():
        for id_ij in hash_IJ2operaciones[cod_op].keys():
            hash_IJ2operaciones_flat[id_ij] = hash_IJ2operaciones[cod_op][id_ij]

    for i,id_ij in enumerate(l_ij_target):

        hash_oper = hash_IJ2operaciones_flat[id_ij]

        observ = hash_oper['Observaciones']

        l_hash_benf = hash_IJ2beneficiarios_flat[id_ij]

        print("Tabla maestra, IJ %d/%d, %d beneficiarios" % (i+1,len(l_ij_target),len(l_hash_benf)))

        for hash_benf in l_hash_benf:

            nif = hash_benf['NIF Destinatario normalizado']

            if nif in hash_NIF:
                continue

            hash_NIF[nif] = True
            
            nombre    = hash_benf['Nombre Destinatario']

            id_coffee = hash_benf['Código Actuación']

            provincia = hash_proyectos.get(id_coffee,{'Provincia':''})['Provincia']
            ccaa      = hash_proyectos.get(id_coffee,{'CCAA':''})['CCAA']
        
            df.loc[len(df)] = [nif,nombre,provincia,ccaa,observ]

    return df.sort_values(by=['NIF'])

def crea_tabla_maestra_UTPRTR(l_id_ij_target,hash_IJ2beneficiarios,hash_IJ2operaciones,hash_proyectos):

    hash_importe_beneficiario       = {}
    hash_importe_beneficiario_final = {}
    hash_beneficiario               = {}
    hash_beneficiario2ij            = {}
    hash_ij                         = {}

    hash_IJ2beneficiarios_flat = {}
    
    for cod_op in hash_IJ2beneficiarios.keys():
        for id_ij in hash_IJ2beneficiarios[cod_op].keys():
            hash_IJ2beneficiarios_flat[id_ij] = hash_IJ2beneficiarios[cod_op][id_ij]

    hash_IJ2operaciones_flat = {}

    for cod_op in hash_IJ2operaciones.keys():
        for id_ij in hash_IJ2operaciones[cod_op].keys():
            hash_IJ2operaciones_flat[id_ij] = hash_IJ2operaciones[cod_op][id_ij]
    
    for i,id_ij in enumerate(l_id_ij_target):

        hash_oper = hash_IJ2operaciones_flat[id_ij]

        observ = hash_oper['Observaciones']
        
        l_hash_benf = hash_IJ2beneficiarios_flat[id_ij]

        print("Tabla maestra UTPRTR, IJ %d/%d, %d beneficiarios" % (i+1,len(l_id_ij_target),len(l_hash_benf)))

        for hash_benf in l_hash_benf:

            id_coffee = hash_benf['Código Actuación']

            nif       = hash_benf['NIF Destinatario normalizado']
            nombre    = hash_benf['Nombre Destinatario']
            importe   = float(hash_benf['Importe total Destinatarios'])
            provincia = hash_proyectos.get(id_coffee,{'Provincia':''})['Provincia']
            ccaa      = hash_proyectos.get(id_coffee,{'CCAA':''})['CCAA']

            hash_beneficiario2ij.setdefault(nif,[]).append(id_ij)

            hash_beneficiario[nif] = [nif,nombre,provincia,ccaa,observ]

            tipo_op = hash_benf['Tipo Operación']

            if tipo_op == "Otros – Especificar":
                tipo_op_n = "Aportaciones dinerarias"
            elif  tipo_op == "Modificaciones de créditos":
                tipo_op_n = "Aportaciones dinerarias"
            else:
                tipo_op_n = tipo_op
        
            hash_ij.setdefault(tipo_op_n,tipo_op)
        
            hash_importe_beneficiario.setdefault(nif,{})
            hash_importe_beneficiario_final.setdefault(nif,{})

            hash_importe_beneficiario[nif].setdefault(tipo_op_n,0)
            hash_importe_beneficiario_final[nif].setdefault(tipo_op_n,0)

            hash_importe_beneficiario[nif][tipo_op_n] = hash_importe_beneficiario[nif][tipo_op_n]+importe

            if hash_oper['Destino Subproyecto'] == "N" or tipo_op == "Encargo a medio propio":
                hash_importe_beneficiario_final[nif][tipo_op_n] = hash_importe_beneficiario_final[nif][tipo_op_n]+importe
    
    l_ij   = sorted(list(set(hash_ij.keys())))
    l_cols = ['NIF','BENEFICIARIO','PROVINCIA','CCAA','CÓDIGO ÚNICO IJ/OPERACIONES','SUMA TOTAL','SUMA TOTAL DEST. FINAL'] + list(reduce(lambda x, y: x + y, list(map(lambda ij: ("IMPORTE TOTAL %s" % (ij), "IMPORTE TOTAL %s DEST. FINAL" % (ij)), l_ij)))) + ['OBSERVACIÓN']
    
    df = pd.DataFrame(columns=l_cols)

    for nif in sorted(hash_beneficiario.keys()):
        
        l_row = hash_beneficiario[nif]

        l_row_final = l_row[:-1]

        l_row_final.append('|'.join(hash_beneficiario2ij[nif]))

        l_impt1 = []
        l_impt2 = []
        
        for ij in l_ij:

            if ij in hash_importe_beneficiario[nif]:
                l_impt1.append(hash_importe_beneficiario[nif][ij])
                l_impt2.append(hash_importe_beneficiario_final[nif][ij])
            else:
                l_impt1.append('.')
                l_impt2.append('.')

        sum1 = sum(float(x) if isinstance(x, (int, float)) else 0 for x in l_impt1)
        sum2 = sum(float(x) if isinstance(x, (int, float)) else 0 for x in l_impt2)
        
        l_row_final.append(formatea_numero(sum1))
        l_row_final.append(formatea_numero(sum2))

        for (impt1,impt2) in zip(l_impt1,l_impt2):
            l_row_final.append(formatea_numero(impt1))
            l_row_final.append(formatea_numero(impt2))
        
        l_row_final.append(l_row[-1])

        df.loc[len(df)] = l_row_final
    
    return df.sort_values(by=['SUMA TOTAL'])

def crea_tabla_beneficiarios_IJ(hash_col2fields,l_id_ij_target,hash_IJ2beneficiarios,hash_id2provisional,hash_proyectos,**kwargs):

    hash_bdns = {}

    if 'BDNS' in kwargs:
        hash_bdns = kwargs['BDNS']
    
    df = pd.DataFrame(columns=list(hash_col2fields.keys()))

    for id_ij in l_id_ij_target:

        if id_ij not in hash_IJ2beneficiarios:
            continue

        l_hash_benf = hash_IJ2beneficiarios[id_ij]
        
        for hash_benf in l_hash_benf:
        
            id_coffee = hash_benf['Código Actuación']
            id_coffee_prov = hash_id2provisional.get(id_coffee,id_coffee+'.REV')

            hash_proy = hash_proyectos.get(id_coffee,{})
            
            l_row = []

            for col in hash_col2fields.keys():

                if col == "CODIGO_BDNS" and hash_bdns != {}:
                    l_row.append(hash_bdns.get(id_ij,""))
                    continue

                field = hash_col2fields[col]
            
                if field == "Código Iniciativa":
                    valor = id_coffee_prov
                elif field in hash_benf:
                    valor = hash_benf[field]
                elif field in hash_proy:
                    valor = hash_proy[field]
                else:
                    valor = ''
            
                if col.find('IMPORTE') >= 0:
                    valor = formatea_numero(valor)
                elif col == "Perceptor final (SI/NO)":
                    valor = formatea_perceptor_final(valor)
                elif col == "ES_SUBCONTRATISTA (SI/NO)":
                    valor = formatea_subcontratista(valor)

                l_row.append(valor)
   
            df.loc[len(df)] = l_row
    
    return df.sort_values(by=['CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)'])

def crea_tabla_IJ(hash_col2fields,l_id_ij_target,hash_IJ2operaciones,hash_id2provisional,hash_proyectos):
    
    df = pd.DataFrame(columns=list(hash_col2fields.keys()))
    
    for id_ij in l_id_ij_target:

        if id_ij not in hash_IJ2operaciones:
            continue

        hash_oper = hash_IJ2operaciones[id_ij]

        cod_coffee      = hash_oper['Código iniciativa']
        cod_coffee_prov = hash_id2provisional.get(cod_coffee,cod_coffee+'.REV')
        
        hash_proy = hash_proyectos.get(cod_coffee,{})

        l_row = []

        for col in hash_col2fields.keys():

            field = hash_col2fields[col]
            
            if field == "Código Iniciativa":
                valor = cod_coffee_prov
            elif field in hash_oper:
                valor = hash_oper[field]
            elif field in hash_proy:
                valor = hash_proy[field]
            else:
                valor = '.'
            
            if col.find('IMPORTE') >= 0:
                valor = formatea_numero(valor) 
                        
            l_row.append(valor)
   
        df.loc[len(df)] = l_row
    
    return df.sort_values(by=['CODIGO_ACTUACION (PROYECTO O SUBPROYECTO)'])

#######################################################

def obtiene_BDNS(hash_IJ2operaciones):

    hash_bdns = {}

    for tipo_op in hash_IJ2operaciones.keys():
        for id_ij in hash_IJ2operaciones[tipo_op].keys():
            hash_bdns[id_ij] = hash_IJ2operaciones[tipo_op][id_ij]['Código BDNS']
    
    return hash_bdns

#######################################################

def obtiene_lista_ij(l_hash_beneficiarios):

    hash_id_ij_target = {}

    num_ben = 0
    
    for elem_benf in l_hash_beneficiarios:
        for id_ij in elem_benf.keys():
            hash_id_ij_target[id_ij] = True
            num_ben += len(elem_benf[id_ij])

    return list(hash_id_ij_target.keys()),num_ben

#######################################################

def main(logger):
    
    parser = argparse.ArgumentParser(description='Formateo de la información de beneficiarios para la carga en SIGEFE')

    parser.add_argument('--input', '-i', type=str, default=None, help='Ruta de la carpeta donde buscar archivos .xlsx de beneficiarios CoFFEE de entrada')
    parser.add_argument('--proyectos', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de proyectos')
    parser.add_argument('--operaciones', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de operaciones')
    parser.add_argument('--pt', type=str, default=[],action='append',help='Proyectos diana. La salida se restringe al proyecto específico configurado')
    parser.add_argument('--output', '-o', type=str, default=None, help='Ruta y nombre del fichero de salida .xlsx con la tabla agregada')
        
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
    
    ### Lista de proyectos 'target' para acotar la salida
    ### La lista puede estar vacía. Eso significa que se incluyen todos los proyectos
    l_proyectos_target = args.pt
       
    logger.info("Leyendo tablas de entrada de CoFFEE")

    # Lee y parsea la tabla con la información de proyectos y subproyectos 'Desembolsos - Listado Total - Relación de Proyectos, Subproyectos, Subproyectos Instrumentales y Actuaciones.xlsx'
    # Input:
    # Además del fichero excel con los proyecto, el usuario puede acotar los proyectos de salida
    # Output:
    # 1. hash_proyectos: Es un diccionario con la tabla completa e indexada como key la información del campo 'Código Iniciativa'
    # 2. hash_id2provisional: Es un diccionario que mapea el campo 'Código Iniciativa' con 'Código provisional iniciativa'
    hash_id2provisional,hash_proyectos = read_CoFFEE_proyectos(input_proyectos,l_proyectos_target)

    # Lee y parsea las tablas de depositarios asociados con cada operación o IJ: 'Desembolsos - Destinatarios con su operación_bloque*.xlsx'
    # Las descargas se hacen por bloques y todas se encuentran en un directorio
    # Output:
    # 1. hash_IJ2beneficiario: Es un diccionario cuya key es el campo 'Tipo Operación' del que cuelga 'Código único IJ' y que a su vez, incluye cada fila en otro diccionario con la estructura: <nombre columna>:<valor>
    hash_IJ2beneficiarios = read_CoFFEE_beneficiarios(input_dir,hash_id2provisional,l_proyectos_target)
        
    # Lee y parsea la tabla con la lista de operaciones (IJ) total: 'Desembolsos - IJOperaciones.xlsx'
    # Output:
    # 1. hash_IJ2operaciones: Es un diccionario con la tabla completa cuya key es 'Código único IJ/Operaciones'
    hash_IJ2operaciones = read_CoFFEE_IJ(input_operaciones,hash_id2provisional,l_proyectos_target)
    
    # A partir del hash_IJ2operaciones genera otro diccionario que relaciona el código único IJ con el código BDNS
    # hash_BDNS es el output y se usa como una estructura intermediaria 
    hash_BDNS = obtiene_BDNS(hash_IJ2operaciones)

    hash_IJ2beneficiarios_AD, hash_IJ2operaciones_AD = obtiene_aportaciones_dinerarias(hash_IJ2beneficiarios,hash_IJ2operaciones)

    logger.info("Lectura finalizada")

    ### Selecciona beneficiarios
    logger.info("Seleccionando lista de beneficiarios de interés")

    l_id_ij_target, num_tot_ben = obtiene_lista_ij([hash_IJ2beneficiarios.get('Subvención',{}),hash_IJ2beneficiarios.get('Contrato',{}),hash_IJ2beneficiarios.get('Convenio',{}),hash_IJ2beneficiarios.get('Encargo a medio propio',{}),hash_IJ2beneficiarios_AD])

    logger.info("Seleccionada la lista")

    logger.info("Número total de IJ: %d" % (len(l_id_ij_target)))
    logger.info("Número total de Beneficiarios: %d" % (num_tot_ben))

    ### Genera las tablas y los asigna a los nombres de las pestañas
    logger.info("Creando las tablas para escribir en el excel")

    hash_df = {}
    
    hash_df['TABLA MAESTRA']               = crea_tabla_maestra(l_id_ij_target,hash_IJ2beneficiarios,hash_IJ2operaciones,hash_proyectos)
    hash_df['TABLA MAESTRA UTPRTR']        = crea_tabla_maestra_UTPRTR(l_id_ij_target,hash_IJ2beneficiarios,hash_IJ2operaciones,hash_proyectos)
         
    hash_df['SUBVENCIONES']                = crea_tabla_IJ(get_cols_subvenciones(),l_id_ij_target,hash_IJ2operaciones.get('Subvención',{}),hash_id2provisional,hash_proyectos)
    hash_df['BENEFICIARIOS_SUBVENCIONES']  = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_subvenciones(),l_id_ij_target,hash_IJ2beneficiarios.get('Subvención',{}),hash_id2provisional,hash_proyectos,BDNS=hash_BDNS)
        
    hash_df['CONTRATOS']                   = crea_tabla_IJ(get_cols_contratos(),l_id_ij_target,hash_IJ2operaciones.get('Contrato',{}),hash_id2provisional,hash_proyectos)
    hash_df['BENEFICIARIOS_CONTRATOS']     = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_contratos(),l_id_ij_target,hash_IJ2beneficiarios.get('Contrato',{}),hash_id2provisional,hash_proyectos)
        
    hash_df['CONVENIOS']                   = crea_tabla_IJ(get_cols_convenios(),l_id_ij_target,hash_IJ2operaciones.get('Convenio',{}),hash_id2provisional,hash_proyectos)
    hash_df['BENEFICIARIOS_CONVENIOS']     = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_convenios(),l_id_ij_target,hash_IJ2beneficiarios.get('Convenio',{}),hash_id2provisional,hash_proyectos)
        
    hash_df['ENCARGOS']                    = crea_tabla_IJ(get_cols_encargo(),l_id_ij_target,hash_IJ2operaciones.get('Encargo a medio propio',{}),hash_id2provisional,hash_proyectos)
    hash_df['BENEFICIARIOS_ENCARGOS']      = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_encargo(),l_id_ij_target,hash_IJ2beneficiarios.get('Encargo a medio propio',{}),hash_id2provisional,hash_proyectos)
    
    hash_df['APORTACIONES_DIN']               = crea_tabla_IJ(get_cols_aportaciones_dinerarias(),l_id_ij_target,hash_IJ2operaciones_AD,hash_id2provisional,hash_proyectos)
    hash_df['BENEFICIARIOS_APORTACIONES_DIN'] = crea_tabla_beneficiarios_IJ(get_cols_beneficiarios_aportaciones_dinerarias(),l_id_ij_target,hash_IJ2beneficiarios_AD,hash_id2provisional,hash_proyectos)

    logger.info("Tablas creadas")
    
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
        

