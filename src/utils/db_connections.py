
import duckdb
from pathlib import Path
from duckdb import DuckDBPyConnection

def get_conn():
    # ----------------------------------------
    # Abre e retorna uma conexão DuckDB para o stage informado.
    # ----------------------------------------
    base_dir = Path("infra") / "repositories"
    base_dir.mkdir(parents=True, exist_ok=True)

    db_path = base_dir / f"duckdb-repository.db"

    conn = duckdb.connect(str(db_path))

    return conn, db_path
    
def close_conn(conn: DuckDBPyConnection, db_path: str = 'duckdb-repository.db'):
    # ----------------------------------------
    # Fecha a conexão passada.
    # ----------------------------------------
    try:
        if conn:
            conn.close()
    finally:
        if db_path and db_path.exists():
            db_path.unlink()
            
