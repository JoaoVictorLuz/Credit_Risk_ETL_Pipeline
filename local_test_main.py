import logging
from src.pipeline import ETLPipeline

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    pipeline = ETLPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()
