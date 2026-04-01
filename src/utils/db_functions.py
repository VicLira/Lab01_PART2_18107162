import re
import unicodedata


def _fix_unicode_recursive(s: str, max_iter: int = 5) -> str:
    """
    Corrige múltiplas camadas de encoding incorreto.
    """
    if not isinstance(s, str):
        return s

    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass

    suspicious = re.compile(r'[ÃÂ]')

    for _ in range(max_iter):
        if not suspicious.search(s):
            break
        try:
            s = s.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            break

    return s


def fix_unicode_duckdb(s: str) -> str:
    return _fix_unicode_recursive(s)