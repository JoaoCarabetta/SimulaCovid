#!/usr/bin/python3
import pandas
import pandas_gbq
import pydata_google_auth

def _get_credentials_gbq():

    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = pydata_google_auth.get_user_credentials(
        SCOPES,
        # Set auth_local_webserver to True to have a slightly more convienient
        # authorization flow. Note, this doesn't work if you're running from a
        # notebook on a remote sever, such as over SSH or with Google Colab.
        auth_local_webserver=True,
    )

    return credentials


def to_gbq(df, 
            table_name, 
            schema_name='simula_corona',
            project_id='robusta-lab', 
            **kwargs):
    """
    write a dataframe in Google BigQuery
    """
    
    destination_table = f'{schema_name}.{table_name}'

    pandas_gbq.to_gbq(
        df,
        destination_table,
        project_id,
        credentials=_get_credentials_gbq(),
        **kwargs
    )

def read_gbq(query, 
            project_id='robusta-lab', 
            **kwargs):
    """
    write a dataframe in Google BigQuery
    """

    return pandas_gbq.read_gbq(
        query,
        project_id,
        credentials=_get_credentials_gbq(),
        **kwargs)


