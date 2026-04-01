import time

class Pipeline:
    def __init__(
        self, 
        extractor,
        loader,
        silver_transformer
    ):
        self.extractor = extractor
        self.loader = loader
        self.silver_transformer = silver_transformer
        
    def _time_step(self, name, func, *args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"\t- {name} levou {end - start:.4f} segundos")
        return result
        
    def run(self):
        urls = self._time_step("Extract", self.extractor.extract)
        self._time_step("Load", self.loader.load, urls=urls)
        self._time_step("Transform", self.silver_transformer.transform) # ~ 3294.1405 segundos