# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 20:25:53 2020

@author: remns
"""

"""Format the main datasets used in the projects"""

import os, os.path
import pandas as pd
import numpy as np

def preprocess_data(project_path=None, data_path=None):
    project_path = project_path if project_path else r'F:\Documentos\Python_learning\Dani_colab\Trenes'
    data_path = data_path if data_path else r'F:\Documentos\Python_learning\Dani_colab\Trenes\Data\Datasets'
    os.chdir(project_path)
    
    
    routes=pd.read_csv(data_path+r'\routes.csv')
    stops = pd.read_csv(data_path+'\stops.csv', dtype={'stop_id':str})
    stations = pd.read_csv(data_path + '\listado-estaciones-completo.csv', sep=';', dtype={'CÓDIGO':str})
    
    stops.drop(columns=['stop_code', 'stop_desc', 'zone_id'], inplace=True)
    stops.sort_values('stop_id', inplace = True)
    
    routes['Depart_ID'] = routes['route_id'].str.slice(0,5)
    routes['Destination_ID'] = routes['route_id'].str.slice(5,10)
    routes['Depart'] = routes['Depart_ID'].map(stops.set_index('stop_id')['stop_name'])
    routes.head()
    routes['Destination'] = routes['Destination_ID'].map(stops.set_index('stop_id')['stop_name'])
    routes.drop(['agency_id', 'route_long_name', 'route_desc', 'route_type', 'route_url', 'route_color', 'route_text_color'], axis=1, inplace=True)
    routes.rename(columns={'route_short_name':'route_type'})
    
    mask = (stations['CÓDIGO'].str.len() == 4)
    stations.loc[mask, 'CÓDIGO'] = '0' + stations['CÓDIGO'].astype(str)
    stations.set_index('CÓDIGO', inplace=True)
    #stations['simple_loc'] = stations.POBLACION.str.split(',', 0, expand=True)[0]
    stations['POBLACION'] = stations['POBLACION'].str.upper()
    expansion = stations.POBLACION.str.split(',', 0, expand=True)
    stations['simple_loc'] = expansion[0]
    stations['article'] = expansion[1].str.split(' ', 0, expand=True)[1]
    
    return routes, stops, stations
