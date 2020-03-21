#!/usr/bin/python3

import requests
import json
import pandas as pd
from paths import RAW_PATH
from datetime import datetime as dt

def covid_cases():
    """
    Collect & save COVID-19 cases dataset
    """
    
    # Get data from API
    url = "https://brasil.io/api/dataset/covid19/caso/data?format=json"
    response = requests.get(url)

    data = response.text
    parsed = json.loads(data)
    df = pd.DataFrame(parsed['results'])

    # Get current date & time
    today = str(dt.today())
    df['last_update'] = today
    
    # Change to pt-br
    df = df.rename({'city': 'municipio', 'state': 'uf'}, axis=1)
    df['municipio'] = df['municipio'].fillna('')

    # Save/update raw dataset
    df.to_csv(RAW_PATH / 'covid19_cases.csv', index=False)
    
    return df

    
if __name__ == '__main__':
    get_cases()
