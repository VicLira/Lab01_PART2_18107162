from dotenv import load_dotenv

from extract.loader import Loader
from pipeline.pipeline import Pipeline
from extract.extractor import Extractor
from transform.silver.transformer import Transformer as SilverTransformer



def main():
    print("Iniciando Pipeline Extração Preços Combustível + GLP")
    pipeline = Pipeline(
                    extractor=Extractor(),
                    loader=Loader(),
                    silver_transformer=SilverTransformer()
                )
    pipeline.run()


if __name__ == "__main__":
    load_dotenv(override=True)
    main()
