#!/usr/bin/python3

import requests
import json
import pandas as pd
from paths import RAW_PATH
from datetime import datetime as dt
import numpy as np
from src import io, clean_data



def city_cases_covid19br(path):
    """
    Collect & save COVID-19 cases dataset from covid19br repo
    """
    
    df = pd.read_csv(path)
    df = df.sort_values(by='date', ascending=False)
    
    # Get current date & time
    today = str(dt.today())
    df['last_update'] = today
    
    # Save/update raw dataset
    df.to_csv(RAW_PATH / 'city_cases_covid19br.csv', index=False)
    
    return df#[cols]

def city_cases_brasilio(url):
    """
    Collect & save COVID-19 cases dataset from Brasil.IO
    """
    
    # Get data from API
    df_final = pd.DataFrame()

    while url != None:
        
        response = requests.get(url)
        data = response.text
        parsed = json.loads(data)
        url = parsed['next']
        df = pd.DataFrame(parsed['results']).sort_values(by='confirmed',ascending=False)
        df_final = pd.concat([df_final,df], axis=0)

    # Get current date & time
    today = str(dt.today())
    df_final['last_update'] = today

    # Save/update raw dataset
    # df_final.to_csv(RAW_PATH / 'city_cases_brasilio.csv', index=False)
    
    return df_final


def build_region_id(_df, city_col='city_name', state_col='state'):

    _df['region_id'] = _df.apply(lambda row: 
                                     clean_data.create_region_id(row[city_col], 
                                                                 row[state_col]), axis=1)
    _df['region_id'] = clean_data.normalize_cols(_df['region_id']) 

    _df = _df[_df[city_col] != _df[state_col]]
     
    return _df


def load_data(config):
    
    df_cases_brasilio = city_cases_brasilio(config['get_data_paths']['cases_brasilio'])
    df_cases_brasilio = clean_data.treat_brasilio(df_cases_brasilio) 
    

    sus_cap = io.read_gbq('select * from `robusta-lab.simula_corona_prod.sus_capacity`')
    sus_regions = io.read_gbq('select * from `robusta-lab.simula_corona_prod.sus_regions`')

    sus_cap = build_region_id(sus_cap, 'municipio', 'uf')
    sus_regions = build_region_id(sus_regions)
    
    cities_cases = clean_data.treat_cities_cases(df_cases_brasilio, sus_cap, sus_regions)
    
    io.to_gbq(cities_cases, 'cities_cases', schema_name='simula_corona_prod',if_exists='replace')

    return cities_cases




    
# if __name__ == '__main__':
#     get_cases()
