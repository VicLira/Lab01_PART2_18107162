
from data_sources.anp_urls import POSSIBLE_LINKS
from extract.extractor import Extractor


def test_all_urls_are_generated():
    generated = set(Extractor.extract())
    expected = set(POSSIBLE_LINKS)
    
    missing = expected - generated
    extra = generated - expected
    
    assert not missing, f"URLs faltando: {missing}"
    assert not extra, f"URLs inesperadas: {extra}"
    assert len(generated) == len(expected)