from dotenv import load_dotenv

from extract.loader import Loader
from pipeline.pipeline import Pipeline
from extract.extractor import Extractor



def main():
    print("Hello from lab01-part2-18107162!")
    pipeline = Pipeline(
                    extractor=Extractor(),
                    loader=Loader()
                )
    valid_files = pipeline.run()
    print(valid_files)


if __name__ == "__main__":
    load_dotenv(override=True)
    main()
