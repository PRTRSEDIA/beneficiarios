"""
Script creado por Angela del Pozo
07/10/2025

Carga tablas de IJ/beneficiarios y extrae las URLs de los contratos

"""

import os
import pandas as pd
import argparse

import warnings

import logging

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def read_CoFFEE_operaciones(input_file):

    l_fields = ['Código único IJ/Operaciones','Código iniciativa','Tipo actuación','URL licitación']

    df = pd.read_excel(input_file, header=2)
    
    hash_operaciones = {}

    for i,row in df[l_fields].iterrows():

        hash_row = dict(zip(l_fields,list(map(str,list(row)))))

        if hash_row['Tipo actuación'] == 'Contrato':

            hash_operaciones[row[0]] = hash_row

    return l_fields, hash_operaciones

def main():
    
    parser = argparse.ArgumentParser(description='Formateo de la información de beneficiarios para la carga en SIGEFE')

    parser.add_argument('--output', '-o', type=str, default=None, help='Ruta y nombre del fichero de salida .xlsx con la tabla agregada')
    parser.add_argument('--operaciones', type=str, default=None, help='Ruta y nombre del fichero de entrada .xlsx con la relación de operaciones')
        
    args = parser.parse_args()
    
    output_file = args.output

    output_dir = os.path.dirname(output_file)

    if not os.path.exists(output_dir):
        raise IOError('El directorio donde se va a escribir el excel de salida no existe: %s' % (output_dir))
    
    input_operaciones = args.operaciones

    if not os.path.exists(input_operaciones):
        raise IOError('El xlsx de entrada con las operaciones, no existe: %s' % (input_operaciones))
    
    l_fields, hash_operaciones = read_CoFFEE_operaciones(input_operaciones)

    df = pd.DataFrame(columns=l_fields)

    for i,id_ij in enumerate(hash_operaciones.keys()):
        
        hash_operacion = hash_operaciones.get(id_ij,{})

        l_row = []

        for f in l_fields:
            l_row.append(hash_operacion[f])

        df.loc[len(df)] = l_row

    df.to_excel(output_file, sheet_name="URL contratos", index=False)
    
if __name__ == '__main__':

    try:
        main()
    except Exception as e:
        print(f"Error general en la ejecución: {e}")
        

