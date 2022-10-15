# LISTAR LOS ARCHIVOS QUE CONTIENE UNA CARPETA (DIRECTORIO) ========================================

'''
import os

Input:
    path_dir = '/opt/ml/processing/data' # Ruta de la carpeta

    opt [carpeta]
    └─── ml [carpeta]
         └─── processing [carpeta]
              └─── data [carpeta]
                   └─── data_1.csv [archivo]
                   └─── data_2.csv [archivo]

Output:
    paths_files = ['/opt/ml/processing/data/data_1.csv', # Rutas de los archivos dentro de la carpeta
                   '/opt/ml/processing/data/data_2.csv']
'''

def list_files(path_dir):
    import os

    paths_files = [] # Rutas de los archivos dentro de la carpeta

    for name_file in os.listdir(path_dir):
        path_file = os.path.join(path_dir, name_file) # '/path_dir/name_file'

        if os.path.isfile(path_file): # Si es un archivo
            paths_files.append(path_file)

    return paths_files

# CARGAR ARCHIVOS CSV DE UNA CARPETA EN UN DATAFRAME ===============================================

'''
! Usa la funcion 'list_files'

import pandas as pd

Input:
    path_dir = '/opt/ml/processing/data' # Ruta de la carpeta con los archivos csv
    row_header = 0 # Fila que contiene la cabecera de cada archivo csv

    opt [carpeta]
    └─── ml [carpeta]
         └─── processing [carpeta]
              └─── data [carpeta]
                   └─── data_1.csv [archivo]
                   └─── data_2.csv [archivo]

    /opt/ml/processing/data/data_1.csv:
        col_0,col_1,col_2
        A00,A01,A02
        A10,A11,A12

    /opt/ml/processing/data/data_2.csv:
        col_0,col_1,col_2
        B00,B01,B02

    ! Si dentro de la ruta 'path_dir' tambien hay archivos que no son csv, estos seran ignorados

Outout:
    type(df) = pandas.core.frame.DataFrame

    | col_0 | col_1 | col_2 |
    |-------|-------|-------|
    | A00   | A01   | A02   |
    | A10   | A11   | A12   |
    | B00   | B01   | B02   |
'''

def load_csv(path_dir, row_header=0):
    import pandas as pd

    dfs = []

    paths_files = list_files(path_dir) # Rutas de los archivos dentro de la carpeta

    for path_file in paths_files:
        file_extension = path_file[-4:]

        if file_extension == '.csv': # Si es un archivo csv
            df = pd.read_csv(path_file, header=row_header)
            dfs.append(df)

    df = pd.concat(dfs)

    return df

# CARGAR ARCHIVOS PARQUET DE UNA CARPETA EN UN DATAFRAME ===========================================

'''
import dask.dataframe as dd

Input:
    path_dir = '/opt/ml/processing/data/*' # Ruta de la carpeta con los archivos parquet

    opt [carpeta]
    └─── ml [carpeta]
         └─── processing [carpeta]
              └─── data [carpeta]
                   └─── 36555063-8359-4060-94bd-b6871f7ef5a9 [archivo]
                   └─── a75f94c8-6fe4-408f-8305-65582dbd755f [archivo]
                   └─── ed5635fc-71dc-4592-8c44-a1247902d110 [archivo]
                   └─── f6105545-e4bc-4954-a9d9-e14565f15ecc [archivo]

    ! El '*' al final de la ruta significa que leera todos los archivos dentro de la carpeta
    ! Tambien se puede usar '*/*' cuando los archivos parquet estan particionados en diferentes carpetas

Output:
    type(df) = pandas.core.frame.DataFrame
'''

def load_parquet(path_dir):
    import dask.dataframe as dd

    df = dd.read_parquet(path_dir) # type(df) = dask.dataframe.core.DataFrame
    df = df.compute() # type(df) = pandas.core.frame.DataFrame

    return df

# ESTABLECER LOS TIPOS DE DATOS DE LAS COLUMNAS DE UN DATAFRAME ====================================

'''
Input:
    type(df) = pandas.core.frame.DataFrame

    | col_0 | col_1 | col_2 |
    |-------|-------|-------|
    | 0     | 0.1   | A     |
    | 1     | 0.2   | B     |
    | 2     | 0.3   | C     |

    df.dtypes = col_0    object
                col_1    object
                col_2    object

    cols = ['col_0', 'col_1', 'col_2']
    dtypes = ['int64', 'float64', 'object']

Output:
    type(df) = pandas.core.frame.DataFrame

    | col_0 | col_1 | col_2 |
    |-------|-------|-------|
    | 0     | 0.1   | A     |
    | 1     | 0.2   | B     |
    | 2     | 0.3   | C     |

    df.dtypes = col_0      int64
                col_1    float64
                col_2     object
'''

def set_dtypes(df, cols, dtypes):
    cols_dtypes = {}

    for col, dtype in zip(cols, dtypes):
        cols_dtypes[col] = dtype
    
    print(cols_dtypes)
    df = df.astype(cols_dtypes)

    return df

# CARGAR ARCHIVO CSV Y SU HEADER EN UN DATAFRAME ===================================================

'''
! Usa la funcion 'load_csv' y 'set_dtypes'

import pandas as pd

Input:
    path_csv = '/opt/ml/processing/data/data.csv'
    path_header = '/opt/ml/processing/header/header.csv'
    is_dir = False # Si las rutas del archivo csv y su cabecera son directorios

    /opt/ml/processing/data/data.csv:
        0,0.75
        1,0.25

    /opt/ml/processing/header/header.csv:
        column,dtype
        col_0,int64
        col_1,float64

    col_names = 'column' # Nombre de la columna con los nombres
    col_dtypes = 'dtype' # Nombre de la columna con los tipos de datos

Output:
    type(df) = pandas.core.frame.DataFrame

    | col_0 | col_1 |
    |-------|-------|
    | 0     | 0.75  |
    | 1     | 0.25  |

    df.dtypes = col_0      int64
                col_1    float64
'''

def load_csv_header(path_csv, path_header, col_names='column', col_dtypes='dtype', is_dir=False):
    import pandas as pd

    df = load_csv(path_csv, row_header=None) if is_dir else pd.read_csv(path_csv, header=None)

    header = load_csv(path_header) if is_dir else pd.read_csv(path_header)
    names = list(header[col_names].values)
    dtypes = list(header[col_dtypes].values)
    df.columns = names
    df = set_dtypes(df, names, dtypes)

    return df