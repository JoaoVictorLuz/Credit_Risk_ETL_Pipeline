import snowflake.connector
from pathlib import Path
import shutil
import logging

logger = logging.getLogger(__name__)

class SnowflakeLoader:
    def __init__(self, config):
        self.config = config
        self.temp_dir = Path(config.temp_dir)
    
    def load(self, dataframes):
        conn = None
        cur = None
        
        try:
            # Conectar ao Snowflake
            conn = snowflake.connector.connect(
                user=self.config.snowflake_user,
                password=self.config.snowflake_password,
                account=self.config.snowflake_account,
                warehouse=self.config.snowflake_warehouse,
                database=self.config.snowflake_database,
                schema=self.config.snowflake_schema
            )
            cur = conn.cursor()
            
            # Verificar conexão
            cur.execute("SELECT CURRENT_VERSION()")
            version = cur.fetchone()[0]
            logger.info(f"Conectado ao Snowflake. Versão: {version}")
            
            # Preparar stage
            cur.execute(f"CREATE STAGE IF NOT EXISTS {self.config.stage_name};")
            logger.info(f"Stage '{self.config.stage_name}' pronto.")
            
            # Salvar parquets
            self.temp_dir.mkdir(parents=True, exist_ok=True)
            parquet_files = []
            
            for name, df in dataframes.items():
                file_path = self.temp_dir / f"{name}.parquet"
                logger.info(f"Salvando {name}.parquet...")
                df.to_parquet(file_path, index=False)
                parquet_files.append(file_path)
            
            # Upload para stage
            for parquet_file in parquet_files:
                put_cmd = f"PUT 'file://{parquet_file.resolve()}' @{self.config.stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                logger.info(f"Enviando {parquet_file.name} para stage...")
                cur.execute(put_cmd)
            
            # Carregar nas tabelas
            for parquet_file in parquet_files:
                table_name = parquet_file.stem.lower()
                logger.info(f"Carregando {table_name}...")
                
                cur.execute(f"TRUNCATE TABLE IF EXISTS {table_name};")
                
                copy_into_sql = f"""
                COPY INTO {table_name}
                FROM @{self.config.stage_name}/{parquet_file.name}
                FILE_FORMAT = parquet_format
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                """
                cur.execute(copy_into_sql)
                logger.info(f"✔ Dados carregados em {table_name}")
            
            # Limpar arquivos temporários
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                logger.info(f"Pasta '{self.temp_dir}' removida")
                
        except Exception as e:
            logger.error(f"Erro no carregamento: {e}")
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            logger.info("Conexão encerrada")
