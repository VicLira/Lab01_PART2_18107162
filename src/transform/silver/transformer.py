

import os
import glob
import duckdb

from utils.db_functions import fix_unicode_duckdb
from utils.db_connections import close_conn, get_conn



class Transformer:
    # =============================
    # CONEXÃO E REGISTROS
    # =============================
    def _register_functions(self, conn: duckdb.DuckDBPyConnection) -> None:
        print("\t- REGISTRANDO FUNÇÕES AUXILIARES")
        
        conn.create_function("FIX_UNICODE", fix_unicode_duckdb)
        conn.execute("""
            CREATE OR REPLACE MACRO SAFE_DATE(s) AS (
                CASE
                    WHEN LENGTH(TRIM(s)) = 0 THEN NULL
                    ELSE COALESCE(
                        TRY_CAST(TRY_STRPTIME(s, '%d/%m/%Y') AS DATE),
                        TRY_CAST(TRY_STRPTIME(s, '%Y-%m-%d') AS DATE)
                    )
                END
            );
        """)
        
    # =============================
    # BRONZE RAW
    # =============================

    _SELECT_COLUMNS = """
            SELECT 
                filename,
                "Regiao - Sigla"        AS regiao_sigla,
                "Estado - Sigla"        AS estado_sigla,
                "Municipio"             AS municipio,
                "Revenda"               AS revenda,
                "CNPJ da Revenda"       AS cnpj_revenda,
                "Nome da Rua"           AS nome_rua,
                "Numero Rua"            AS numero_rua,
                "Complemento"           AS complemento,
                "Bairro"                AS bairro,
                "Cep"                   AS cep,
                "Produto"               AS produto,
                "Data da Coleta"        AS data_coleta,
                "Valor de Venda"        AS valor_venda,
                "Valor de Compra"       AS valor_compra,
                "Unidade de Medida"     AS unidade_medida,
                "Bandeira"              AS bandeira
            FROM read_csv(
                {file_list},
                delim=';',
                header=true,
                all_varchar=true,
                quote='"',
                escape='"',
                filename=true,
                encoding='{encoding}',
                ignore_errors=true,
                null_padding=true
            )
    """

    def _detect_encoding(self, conn: duckdb.DuckDBPyConnection, filepath: str) -> str:
        """Detecta encoding testando leitura do header + 1ª linha."""
        for enc in ("utf-8", "iso89"):
            try:
                conn.execute(f"""
                    SELECT * FROM read_csv(
                        '{filepath}',
                        delim=';', header=true, all_varchar=true,
                        quote='"', escape='"',
                        encoding='{enc}', null_padding=true
                    ) LIMIT 1
                """)
                return enc
            except Exception:
                continue
        return "utf-8"

    def _create_bronze_raw(self, conn: duckdb.DuckDBPyConnection, files: list[str]) -> None:
        print(f"\t- CRIANDO TABELA bronze_raw ({len(files)} arquivos)")

        # Fase 1: Detectar encoding por arquivo (rápido, só lê header + 1 linha)
        print("\t- DETECTANDO ENCODING")
        groups: dict[str, list[str]] = {}
        for f in files:
            enc = self._detect_encoding(conn, f)
            groups.setdefault(enc, []).append(f)
            print(f"\t  [{os.path.basename(f)}] {enc.upper()}")

        # Fase 2: Carga em lote por grupo de encoding (rápido, DuckDB lê em paralelo)
        created = False
        for enc, group_files in groups.items():
            file_list = "[" + ", ".join(f"'{f}'" for f in group_files) + "]"
            action = "CREATE OR REPLACE TABLE bronze_raw AS" if not created else "INSERT INTO bronze_raw"

            sql = f"{action}\n{self._SELECT_COLUMNS}"
            conn.execute(sql.format(file_list=file_list, encoding=enc))
            created = True
    
    # =============================
    #  LIMPEZA E SILVER
    # =============================
    def _create_silver(self, conn: duckdb.DuckDBPyConnection) -> None:
        print("\t- APLICANDO LIMPEZA E PADRONIZAÇÃO (SILVER)")
        
        conn.execute("""
            CREATE OR REPLACE TABLE silver AS
            WITH cleaned AS (
                SELECT
                    filename
                    ,UPPER(TRIM(regiao_sigla)) AS regiao_sigla
                    ,UPPER(TRIM(estado_sigla)) AS estado_sigla
                    ,UPPER(TRIM(municipio)) AS municipio
                    ,UPPER(TRIM(revenda)) AS revenda
                    ,REGEXP_REPLACE(cnpj_revenda, '\\D', '', 'g') AS cnpj_revenda
                    ,UPPER(TRIM(nome_rua)) AS nome_rua
                    ,TRIM(numero_rua) AS numero_rua
                    ,UPPER(TRIM(complemento)) AS complemento
                    ,UPPER(TRIM(bairro)) AS bairro
                    ,REGEXP_REPLACE(cep, '\\D', '', 'g') AS cep
                    ,UPPER(TRIM(produto)) AS produto
                    ,SAFE_DATE(data_coleta) AS data_coleta
                    ,TRY_CAST(REPLACE(REPLACE(valor_venda, '.', ''), ',', '.') AS DOUBLE) AS valor_venda
                    ,TRY_CAST(REPLACE(REPLACE(valor_compra, '.', ''), ',', '.') AS DOUBLE) AS valor_compra
                    ,UPPER(TRIM(unidade_medida)) AS unidade_medida
                    ,UPPER(TRIM(bandeira)) AS bandeira
                    ,EXTRACT(YEAR FROM SAFE_DATE(data_coleta)) AS ano
                    ,EXTRACT(MONTH FROM SAFE_DATE(data_coleta)) AS mes
                FROM bronze_raw
            ),
            hashed AS (
                SELECT *,
                    HASH(
                        COALESCE(regiao_sigla, '') || '|' ||
                        COALESCE(estado_sigla, '') || '|' ||
                        COALESCE(municipio, '') || '|' ||
                        COALESCE(revenda, '') || '|' ||
                        COALESCE(cnpj_revenda, '') || '|' ||
                        COALESCE(nome_rua, '') || '|' ||
                        COALESCE(numero_rua, '') || '|' ||
                        COALESCE(complemento, '') || '|' ||
                        COALESCE(bairro, '') || '|' ||
                        COALESCE(cep, '') || '|' ||
                        COALESCE(produto, '') || '|' ||
                        COALESCE(data_coleta::VARCHAR, '') || '|' ||
                        COALESCE(valor_venda::VARCHAR, '') || '|' ||
                        COALESCE(valor_compra::VARCHAR, '') || '|' ||
                        COALESCE(unidade_medida, '') || '|' ||
                        COALESCE(bandeira, '')
                    ) AS row_hash
                FROM cleaned
            )
            SELECT 
                filename
                ,row_hash
                ,regiao_sigla
                ,estado_sigla
                ,municipio
                ,revenda
                ,cnpj_revenda
                ,nome_rua
                ,numero_rua
                ,complemento
                ,bairro
                ,cep
                ,produto
                ,data_coleta
                ,valor_venda
                ,valor_compra
                ,unidade_medida
                ,bandeira
                ,ano
                ,mes
            FROM (
                SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY row_hash) AS rn
                FROM hashed
            )
            WHERE rn = 1 AND row_hash != '12110636399144143840';
        """)
        
    # =============================
    # PERSISTÊNCIA
    # =============================
    
    def _persist_silver(self, conn: duckdb.DuckDBPyConnection, silver_root: str) -> None:
        print("\t- SALVANDO PARQUET (PARTICIONADO POR ANO)")

        os.makedirs(silver_root, exist_ok=True)

        conn.execute(f"""
            COPY silver
            TO '{silver_root}/combustiveis'
            (FORMAT PARQUET, PARTITION_BY (ano), OVERWRITE_OR_IGNORE)
        """)

        print("\t- SILVER SALVA COM SUCESSO")

    # =============================
    # FUNÇÃO PRINCIPAL
    # =============================
    
    
    def transform(self) -> None:
        """
        Pipeline completo Bronze -> Silver
        """
        
        bronze_root = os.path.abspath(os.getenv("BRONZE_DATA_PATH"))
        silver_root = os.path.abspath(os.getenv("SILVER_DATA_PATH"))
        
        csv_files = sorted(glob.glob(os.path.join(bronze_root, "**", "*.csv"), recursive=True))
        conn, db_path = get_conn()
        
        try:
            self._register_functions(conn)
            self._create_bronze_raw(conn, csv_files)
            self._create_silver(conn)
            self._persist_silver(conn, silver_root)
        finally:
            close_conn(conn, db_path)