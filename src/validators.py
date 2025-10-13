# src/validators.py
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validador de qualidade de dados"""
    
    @staticmethod
    def validate_not_empty(df: pd.DataFrame, dataset_name: str = "Dataset") -> None:
        if df.empty:
            raise ValueError(f"{dataset_name}: DataFrame está vazio!")
        
        logger.info(f"{dataset_name}: ✓ {len(df)} registros")
