import ipeadatapy as ipdy
import csv
import boto3
from datetime import datetime
import logging
import sys

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Extração da API
serie_ipea = 'PRECOS12_IPCA12'

try:
    df = ipdy.timeseries(serie_ipea)
    
except Exception as e:
    logging.error(f"Erro ao extrair {serie_ipea}: {e}")
    sys.exit(1)

# Criação do arquivo
filename = f'{serie_ipea}.csv'

# print(df.head())
# print(df.columns)

with open(filename, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['date', 'serie', 'value', 'load_ts'])

    for _, row in df.iterrows():
        writer.writerow(
            [
                row['RAW DATE'],
                serie_ipea,
                row['VALUE (-)'],
                datetime.utcnow().isoformat()
            ]
        )

logging.info(f'Arquivo {filename} criado')

####
# Enviando para a S3
bucket_name = 'study-dbt-pipeline-jpzam'
s3_key = f'raw/ipea/{serie_ipea}.csv'

try:
    s3 = boto3.client(
    's3',
    region_name='us-east-1'
    )

    s3.upload_file(
        filename, bucket_name, s3_key
    )
    logging.info(
        f'Arquivo enviado para s3://{bucket_name}/{s3_key}'
    )
except Exception as e:
    logging.error(f'Erro de envio para o s3: {e}')
    sys.exit(1)