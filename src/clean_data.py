import pandas as pd
from paths import RAW_PATH, TREAT_PATH
from datetime import datetime as dt
# from unidecode import unidecode
import numpy as np

def normalize_cols(df):
    return pd.Series(df).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

def create_region_id(city, state):
    """
    Join and clean city + state names to cross databases.
    """
    
    if city:
        city_state = (state + ' ' + city.upper()).replace(' ', '_').replace("'", '')
        return city_state
    
    return ''

def treat_covid19br(filepath, to_path):
    
    df = pd.read_csv(RAW_PATH / filepath)
    
    # Remove total rows
    df = df[df['state'] != 'TOTAL']

    # Fix columns 
    df = df.rename({'totalCases': 'confirmed', 'newCases': 'new_confirmed'}, axis=1)
    df['place_type'] = 'city'
    
    # Create id for join
    df['region_id'] = df['city'].str.replace('/', ' ')
    df['region_id'] = normalize_cols(df['region_id'])   
    
    # Get only last day data for each city
    df = df.drop_duplicates(subset=['city'], keep='first')
    df = df.sort_values(by='confirmed', ascending=False)

    cols = ['region_id', 'city', 'place_type', 'date', 'confirmed']
    
    # Save treated dataset
    df[cols].to_csv(TREAT_PATH / to_path)
    
    return df[cols]

    
def treat_brasilio(df):
    
    # df = pd.read_csv(RAW_PATH / filepath)
    
    # Fix city names
    df['city'] = df['city'].fillna('').str.replace('\'', '')
    
    # Create id for join
    df['region_id'] = df.apply(lambda row: create_region_id(row['city'], row['state']), axis=1)
    df['region_id'] = normalize_cols(df['region_id']) 

    # get only the cities
    mask = ((df['place_type']=='city') & (df['city_ibge_code'].notnull()))
    df = df[mask].sort_values(by = ['region_id','date'])
    

    df['confirmed_shift'] = df['confirmed'].shift(1)
    df['city_ibge_code_shift'] = df['city_ibge_code'].shift(1)

    df['confirmed_shift'] = np.where(df['city_ibge_code_shift']!=df['city_ibge_code'], np.nan , df['confirmed_shift'])
    df['new_cases'] = df['confirmed'] - df['confirmed_shift']
    df['new_cases'] = np.where(df['confirmed_shift'].isnull(), df['confirmed'] , df['new_cases'])

    mask =   ((df['new_cases']!=0) & (df['new_cases']>0) & (df['is_last']==True))
    df['update'] = np.where(mask,True,False)

    mask = df['is_last']==True
    df = df[mask].rename(columns={"confirmed":'confirmed_real','deaths':'deaths_real'})


    cols = ['region_id', 'date', 'confirmed_real','deaths_real','update']

    return df[cols]
    

def treat_sus(filepath, to_path):
    
    df = pd.read_csv(RAW_PATH / filepath)
    
    # Create id for join
    df['region_id'] = df.apply(lambda row: create_region_id(row['municipio'], row['uf']), axis=1)
    df['region_id'] = normalize_cols(df['region_id']) 
    
    # Add available respirator column
    df['ventiladores_disponiveis'] = df['ventiladores_existentes'] - df['ventiladores_em_uso'] 
    
    cols = ['region_id', 'municipio', 'uf', 'populacao', 'quantidade_leitos', 'ventiladores_existentes']
    df = df[cols].fillna(0)

    # remove apostoflo
    df['municipio'] = df['municipio'].str.replace('\'', '')
    df['region_id'] = df['region_id'].str.replace('\'', '')

    # Get the information for states
    df_states = df.groupby(by='uf', as_index=False).sum()
    df_states['municipio'] = df_states['uf']
    df_states['region_id'] = df_states['uf']

    # Put states in same order as df
    cols = df.columns
    df_states = df_states[cols]

    # Concat states information with sus information
    df = pd.concat([df_states,df],axis=0)
    
    # Save treated dataset
    df.to_csv(TREAT_PATH / to_path)

    return df

def treat_cities_cases(df_cases_brasilio,sus_cap,sus_regions):

    cities_cases = pd.merge(sus_cap, df_cases_brasilio, on='region_id', how='left')\
                    .rename(columns={'municipio':'city_name','uf':'state','date':'last_update'})
    cities_cases = pd.merge(cities_cases, sus_regions[['region_id', 'sus_region_name']], 
                on=['region_id'], how='left')

    cities_cases['confirmed_inputed'] = np.where(cities_cases['confirmed_real'].isnull(),1,cities_cases['confirmed_real'])
    cities_cases['deaths_inputed'] = np.where(cities_cases['deaths_real'].isnull(),0,cities_cases['deaths_real'])
    cities_cases['update'] = cities_cases['update'].fillna(False)

    cols = ['region_id', 'city_name', 'state','sus_region_name',
            'confirmed_real', 'confirmed_inputed','deaths_real', 'deaths_inputed',
        'last_update','quantidade_leitos','ventiladores_existentes','populacao','update']
    
    return cities_cases[cols]

# if __name__ == '__main__':
#     treat_all()