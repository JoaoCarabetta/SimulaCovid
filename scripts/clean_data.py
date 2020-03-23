import pandas as pd
from paths import RAW_PATH, TREAT_PATH
from datetime import datetime as dt
from unidecode import unidecode
import numpy as np

def normalize_cols(df):
    return pd.Series(df).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

def create_region_id(city, state):
    """
    Join and clean city + state names to cross databases.
    """
    
    if city:
        city_state = city.upper() + ' ' + state
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

    
def treat_brasilio(filepath, to_path):
    
    df = pd.read_csv(RAW_PATH / filepath)
    
    # Fix city names
    df['city'] = df['city'].fillna('').str.replace('\'', '')
    
    # Create id for join
    df['region_id'] = df.apply(lambda row: create_region_id(row['city'], row['state']), axis=1)
    df['region_id'] = normalize_cols(df['region_id']) 
    
    # city = state when state data
    mask = df['place_type']=='state'
    df['region_id'] = np.where(mask, df['state'], df['region_id'])
    
    cols = ['region_id', 'city', 'place_type', 'date', 'confirmed']
    
    # Get only last day data for each city
    df = df.drop_duplicates(subset=['city'], keep='first')
    df = df.sort_values(by='confirmed', ascending=False)

    cols = ['region_id', 'city', 'place_type', 'date', 'confirmed']
    
    # Save treated dataset
    df[cols].to_csv(TREAT_PATH / to_path)
    
    return df
    

def treat_sus(filepath, to_path):
    
    df = pd.read_csv(RAW_PATH / filepath)
    
    # Create id for join
    df['region_id'] = df.apply(lambda row: create_region_id(row['municipio'], row['uf']), axis=1)
    df['region_id'] = normalize_cols(df['region_id']) 
    
    # Add available respirator column
    df['ventiladores_disponiveis'] = df['ventiladores_existentes'] - df['ventiladores_em_uso'] 
    
    cols = ['region_id', 'municipio', 'uf', 'populacao', 'quantidade_leitos', 'ventiladores_disponiveis']
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
    

# if __name__ == '__main__':
#     treat_all()