import boto3
import pandas as pd
from io import StringIO
import logging

logger = logging.getLogger(__name__)

class S3Extractor:
    def __init__(self, config):
        self.config = config
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region
        )
    
    def extract(self):
        logger.info(f"Extraindo dados do bucket: {self.config.bucket_name}")
        
        response = self.s3_client.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=self.config.s3_prefix
        )
        
        dataframes = {}
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv'):
                    name = key.split('/')[-1].replace('.csv', '')
                    logger.info(f"Lendo {key}...")
                    
                    obj_data = self.s3_client.get_object(Bucket=self.config.bucket_name, Key=key)
                    content = obj_data['Body'].read().decode('utf-8')
                    
                    if name == 'GermanCredit':
                        df = pd.read_csv(StringIO(content), sep=r'\s+')
                    else:
                        df = pd.read_csv(StringIO(content))
                    
                    dataframes[name] = df
                    logger.info(f"{name}: {df.shape[0]} linhas, {df.shape[1]} colunas")
        
        return dataframes
