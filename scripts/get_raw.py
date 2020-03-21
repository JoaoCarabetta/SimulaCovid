#!/usr/bin/python3

import requests
import json
import pandas as pd
from paths import RAW_PATH
from datetime import datetime as dt

def get_cases():
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
    today = today[:19].replace(' ', '_').replace(':', '')

    # Check for outdated dataset
    df_old = sorted(RAW_PATH.glob('*_covid19_cases.csv'))

    if df_old:
        df_old[0].unlink()

    # Save/update raw dataset
    df.to_csv(RAW_PATH / '{}_covid19_cases.csv'.format(today))
    
    return df

if __name__ == '__main__':
    get_cases()
