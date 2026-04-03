import time

class Pipeline:
    def __init__(
        self, 
        extractor,
        loader,
        silver_transformer,
        schema_creator,
        gold_transformer,
        validator=None,
    ):
        self.extractor = extractor
        self.loader = loader
        self.silver_transformer = silver_transformer
        self.schema_creator = schema_creator
        self.gold_transformer = gold_transformer
        self.validator = validator
        
    def _time_step(self, name, func, *args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"\t- {name} levou {end - start:.4f} segundos")
        return result
        
    def run(self):
        urls = self._time_step("Extract", self.extractor.extract) # ~ 0.0001 segundos
        self._time_step("Load", self.loader.load, urls=urls) # ~ 293.3385 segundos
        if self.validator:
            self._time_step("Quality", self.validator.validate) # Great Expectations
        self._time_step("Transform", self.silver_transformer.transform) # ~ 3294.1405 segundos
        self._time_step("Gold_Schemas", self.schema_creator.main)
        self._time_step("Gold_Transform", self.gold_transformer.transform)