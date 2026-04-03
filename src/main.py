from dotenv import load_dotenv

from extract.loader import Loader
from pipeline.pipeline import Pipeline
from extract.extractor import Extractor
from quality.validator import Validator
from transform.silver.transformer import Transformer as SilverTransformer
from transform.gold.schema_creator import SchemaCreator
from transform.gold.transformer import Transformer as GoldTransformer

def main():
    print("Iniciando Pipeline Extração Preços Combustível + GLP")
    pipeline = Pipeline(
                    extractor=Extractor(),
                    loader=Loader(),
                    silver_transformer=SilverTransformer(),
                    schema_creator=SchemaCreator(),
                    gold_transformer=GoldTransformer(),
                    validator=Validator(),
                )
    pipeline.run()

if __name__ == "__main__":
    load_dotenv(override=True)
    main()
