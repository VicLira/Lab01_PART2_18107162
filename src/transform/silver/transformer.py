

import os
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
    def _create_bronze_raw(self, conn: duckdb.DuckDBPyConnection, path: str) -> None:
        print("\t- CRIANDO TABELA bronze_raw")
        
        ## LENDO TUDO COMO TEXTO PARA EVITAR QUEBRA DE DIFERENTES ENCODINGS ENTRE ARQUIVOS
        conn.execute(f"""
            CREATE OR REPLACE TABLE bronze_raw AS
            SELECT 
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',1), '')  AS regiao_sigla,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',2), '')  AS estado_sigla,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',3), '')  AS municipio,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',4), '')  AS revenda,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',5), '')  AS cnpj_revenda,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',6), '')  AS nome_rua,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',7), '')  AS numero_rua,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',8), '')  AS complemento,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',9), '')  AS bairro,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',10), '') AS cep,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',11), '') AS produto,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',12), '') AS data_coleta,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',13), '') AS valor_venda,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',14), '') AS valor_compra,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',15), '') AS unidade_medida,
                NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',16), '') AS bandeira
            FROM read_csv(
                '{path}',
                delim='^',
                columns={{'DS_LINHA':'VARCHAR'}},
                encoding='ISO8859_1'
            )
        """)
    
    # =============================
    #  LIMPEZA E SILVER
    # =============================
    def _create_silver(self, conn: duckdb.DuckDBPyConnection) -> None:
        print("\t- APLICANDO LIMPEZA E PADRONIZAÇÃO (SILVER)")
        
        conn.execute("""
            CREATE OR REPLACE TABLE silver AS
            WITH cleaned AS (
                SELECT
                    UPPER(TRIM(regiao_sigla)) AS regiao_sigla
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
                        regiao_sigla || '|' ||
                        estado_sigla || '|' ||
                        municipio || '|' ||
                        revenda || '|' ||
                        cnpj_revenda || '|' ||
                        nome_rua || '|' ||
                        numero_rua || '|' ||
                        complemento || '|' ||
                        bairro || '|' ||
                        cep || '|' ||
                        produto || '|' ||
                        data_coleta || '|' ||
                        valor_venda || '|' ||
                        valor_compra || '|' ||
                        unidade_medida || '|' ||
                        bandeira
                    ) AS row_hash
                FROM cleaned
            )
            SELECT 
                row_hash
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
            FROM hashed
            -- FROM (
            --    SELECT *,
            --        ROW_NUMBER() OVER (PARTITION BY row_hash) AS rn
            --    FROM hashed
            -- )
            -- WHERE rn = 1;
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
        
        path = os.path.join(bronze_root, "**", "*.csv")
        conn, db_path = get_conn()
        
        try:
            self._register_functions(conn)
            self._create_bronze_raw(conn, path)
            self._create_silver(conn)
            self._persist_silver(conn, silver_root)
        finally:
            close_conn(conn, db_path)