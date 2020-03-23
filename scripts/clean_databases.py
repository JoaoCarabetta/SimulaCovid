import pandas as pd
from paths import RAW_PATH, TREAT_PATH
from datetime import datetime as dt
from unidecode import unidecode

def join_city_state(city, state):
    """
    Join and clean city + state names to cross databases.
    """
    
    if city:
        city_state = unidecode(city.upper()) + ' ' + state
        return city_state
    
    return ''

def treat(df, filename):
    """
    Treat single database for analysis. 
    """
    
    # Add city + state column to cross
    df['municipio'] = df['municipio'].fillna('')
    df['region_id'] = df.apply(lambda row: join_city_state(row['municipio'], row['uf']), axis=1)
    
    # Save clened database
    treat_name = 'treated_'+filename
    df.to_csv(TREAT_PATH / treat_name)
    
    return df


def treat_all():
    """
    Treat all databases for analysis.
    """
    
    raw_files = sorted(RAW_PATH.glob("*.csv"))
    
    dfs = []
    for filepath in raw_files:

        df = pd.read_csv(filepath)
        filename = filepath.parts[-1]

        dfs.append(treat(df, filename))
        
    return dfs
    

if __name__ == '__main__':
    treat_all()