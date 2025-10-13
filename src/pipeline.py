import logging

#Importacoes locais
from .config import config
from .extract import S3Extractor
from .transform import DataTransformer
from .load import SnowflakeLoader

logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self):
        self.config = config
        self.extractor = S3Extractor(config)
        self.transformer = DataTransformer()
        self.loader = SnowflakeLoader(config)
    
    # Em caso de teste local
    def run(self):
        logger.info("="*60)
        logger.info("INICIANDO PIPELINE ETL")
        logger.info("="*60)
        
        logger.info("\n[1/3] EXTRAÇÃO")
        raw_data = self.extractor.extract()
        logger.info(f"✔ {len(raw_data)} datasets extraídos")
        
        logger.info("\n[2/3] TRANSFORMAÇÃO")
        transformed_data = self.transformer.transform_all(raw_data)
        logger.info(f"✔ {len(transformed_data)} datasets transformados")
        
        logger.info("\n[3/3] CARREGAMENTO")
        self.loader.load(transformed_data)
        logger.info("✔ Dados carregados no Snowflake")
        
        logger.info("="*60)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("="*60)
    
    # Métodos individuais para orquestração
    def extract(self):
        return self.extractor.extract()
    
    def transform(self, raw_data):
        return self.transformer.transform_all(raw_data)
    
    def load(self, transformed_data):
        self.loader.load(transformed_data)
