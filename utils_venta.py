import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

print('import la libreria')
############################################ PREPROCESAMIENTO ###########################################

def str_filling(df, label, value):
    df[label] = df[label].replace([''], value)
    df[label] = df[label].fillna(value)
    return df

def fillna_non_0(df):
    df['edad'] = df['edad'].fillna(df['edad'].median())
    df = str_filling(df, 'ubigeo_buro', 'Otros')
    df['ubigeo_buro'] = df['ubigeo_buro'].str.upper()
    df = str_filling(df, 'grp_riesgociiu', 'grupo_0')
    df['grp_riesgociiu'] = df['grp_riesgociiu'].str.upper()
    df = str_filling(df, 'region', 'Vacio')
    df['region'] = df['region'].str.upper()
    return df

def new_imputations(df):
    df['seg_un']  = pd.Series(np.where(df.seg_un.isin([0,3]),0,df.seg_un))   
    df['grp_riesgociiu']  = pd.Series(np.where(df.grp_riesgociiu.isin(['grupo_2','grupo_3','grupo_9','grupo_8','grupo_1']),'grupo_11',df.grp_riesgociiu))
    return df

def feature_encoding(df):
    features_encoder = ['grp_camptottlv06m','grp_campecstlv06m','grp_camptot06m','grp_campecs06m', 
    'region','ubigeo_buro', 'grp_riesgociiu']
    for c in features_encoder:
      le = LabelEncoder()
      le.fit(df[c].astype(str))
      df[c]=le.transform(df[c].astype(str))            
    return df

def filling_and_imputations(df):
    df = fillna_non_0(df) #custimizada
    df = df.fillna(0)
    df = new_imputations(df) #custimizada
    df = feature_encoding(df)
    return df

def type_setting(args, df_main, df_header, prep_type = 'Training'):
    df_header.columns = df_header.columns.str.lower()
    df_main.columns = df_main.columns.str.lower()
    k_type = df_header.set_index('column')['dtype'].to_dict()
    if prep_type == 'Inference':
      k_type.pop('target')
    print(df_main.columns)
    df_main = df_main.astype(k_type) # Establecer tipos de datos de las columnas# Cargar archivo con el orden y los tipos de datos de las columnas
    return df_main

############################################ SPLIT ###########################################

def set_df_model_and_valid(df_main, meses):
    val_period = meses #[202001, 202002]
    valid_vec = df_main['codmes'].isin(val_period) 
    df_valid = df_main.loc[valid_vec]
    df_model = df_main.loc[~valid_vec] # será df_model
    return df_model, df_valid