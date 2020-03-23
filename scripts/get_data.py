#!/usr/bin/python3

import requests
import json
import pandas as pd
from paths import RAW_PATH
from datetime import datetime as dt
import numpy as np

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

def city_cases_brasilio(path):
    """
    Collect & save COVID-19 cases dataset from Brasil.IO
    """
    
    # Get data from API
    response = requests.get(path)

    data = response.text
    parsed = json.loads(data)
    df = pd.DataFrame(parsed['results'])

    # Get current date & time
    today = str(dt.today())
    df['last_update'] = today

    # Save/update raw dataset
    df.to_csv(RAW_PATH / 'city_cases_brasilio.csv', index=False)
    
    return df

    
# if __name__ == '__main__':
#     get_cases()
