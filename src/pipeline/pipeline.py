class Pipeline:
    def __init__(
        self, 
        extractor,
        loader
    ):
        self.extractor = extractor
        self.loader = loader
        
    def run(self):
        urls = self.extractor.extract()
        valid_files = self.loader.load(urls=urls)
        return valid_files
        