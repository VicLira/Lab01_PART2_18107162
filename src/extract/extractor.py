
from typing import List
from data_sources.anp_urls import POSSIBLE_LINKS

class Extractor:
    
    @classmethod
    def extract(self) -> List[str]:
        """
        Retorna exatamente as URLs oficiais definidas.
        Garante:
        - Sem geração especulativa
        - Sem duplicatas
        - Cobertura total da lista oficial
        """
        
        print("\t- CARREGANDO URLS OFICIAIS DA ANP")
        
        # Remove duplicatas preservando ordem
        urls = list(dict.fromkeys(POSSIBLE_LINKS))
        
        print(f"\t- TOTAL NA LISTA ORIGINAL: {len(POSSIBLE_LINKS)}")
        print(f"\t- TOTAL ÚNICO GERADO: {len(urls)}")
        
        # Validação forte
        if len(urls) != len(POSSIBLE_LINKS):
            raise ValueError("⚠ Existem URLs duplicadas na lista oficial.")
        
        print("\t- VALIDAÇÃO OK: 100% DAS URLS INCLUÍDAS")
        
        return urls