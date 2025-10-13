import os
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()

@dataclass
class Config:
    # AWS
    aws_access_key_id: str = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key: str = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region: str = os.getenv('AWS_REGION')
    bucket_name: str = os.getenv('BUCKET_NAME')
    s3_prefix: str = ''
    
    # Snowflake
    snowflake_user: str = os.getenv('SNOWFLAKE_USER')
    snowflake_password: str = os.getenv('SNOWFLAKE_PASSWORD')
    snowflake_account: str = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_warehouse: str = os.getenv('SNOWFLAKE_WAREHOUSE')
    snowflake_database: str = os.getenv('SNOWFLAKE_DATABASE')
    snowflake_schema: str = os.getenv('SNOWFLAKE_SCHEMA')
    snowflake_role: str = os.getenv('SNOWFLAKE_ROLE')
    stage_name: str = os.getenv('STAGE_NAME')
    
    # Processing
    temp_dir: str = 'temp_parquet_files'

config = Config()

