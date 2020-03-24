#!/usr/bin/python3
import pandas
import pandas_gbq
import pydata_google_auth


def to_gbq(df,table_name):

    """
    write a dataframe in Google BigQuery
    """
    
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


    project_id = 'robusta-lab'
    destination_table = 'simula_corona.{}'.format(table_name)

    pandas_gbq.to_gbq(
        df,
        destination_table,
        project_id,
        credentials=credentials,
        if_exists='replace'
    )

    return('Done!')

