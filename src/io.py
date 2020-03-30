#!/usr/bin/python3
import pandas
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import storage, bigquery
import tempfile

def generate_partition_path(partitions):

    return '/'.join([f'{k}={str(v).replace(" ", "_")}'
             for k, v in partitions.items()]) + '/'

def _get_credentials_gbq():

    credentials = service_account.Credentials.from_service_account_file('../configs/gcloud.json')

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


def _get_bucket(config):
    
    storage_client = storage.Client.from_service_account_json('../configs/gcloud.json')
    return storage_client.get_bucket(config['gcloud']['storage']['bucket'])    

def _drop_partitioned_columns(df, partition):

    for k in partition.keys():
        try:
            df = df.drop(k, 1)
        except:
            continue

    return df

def upload_data(df, table_name, partition, filename, config):
    """Saves dataframe as parquet to GCloud storage"""

    path = generate_partition_path(partition) + filename
    df = _drop_partitioned_columns(df, partition)

    bucket = _get_bucket(config)
    
    destination_path = f"{config['gcloud']['bigquery']['schema']['prod']}/{table_name}/{path}"
    
    blob = bucket.blob(destination_path)
        
    with tempfile.TemporaryDirectory() as tmpdirname:
        
        df.to_parquet(tmpdirname + '/temp', index=False)
        source_path = str(tmpdirname + '/temp')
        
        blob.upload_from_filename(source_path)
        
def _get_bigquery(path='../configs/gcloud.json'):
    return bigquery.Client.from_service_account_json(path)

def create_table(table_name, config, partition=False, schema=None):
    
    bigquery_client = _get_bigquery()
    
    dataset = bigquery_client.dataset(config['gcloud']['bigquery']['schema']['prod'])
    
    uri = f"gs://{config['gcloud']['storage']['bucket']}/{config['gcloud']['bigquery']['schema']['prod']}/{table_name}/*"
    
    job_config = bigquery.LoadJobConfig()

    job_config.source_format = bigquery.SourceFormat.PARQUET

    if schema is None:
        job_config.autodetect = True
    else:
        print('oi')
        job_config.schema = schema


    if partition:
        drop_table(table_name, config) # drop table to refresh partitions
        hive = bigquery.external_config.HivePartitioningOptions()
        hive.mode = 'AUTO'
        hive.source_uri_prefix = f"gs://{config['gcloud']['storage']['bucket']}/{config['gcloud']['bigquery']['schema']['prod']}/{table_name}/"
        job_config.hive_partitioning = hive   
    
    load_job = bigquery_client.load_table_from_uri(uri,
                                               dataset.table(table_name),
                                               job_config=job_config)
    load_job.result()
    
def drop_table(table_name, config):

    bigquery_client = _get_bigquery()
    
    dataset = bigquery_client.dataset(config['gcloud']['bigquery']['schema']['prod'])
    
    bigquery_client.delete_table(dataset.table(table_name), not_found_ok=True)