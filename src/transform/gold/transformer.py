import os
import psycopg2
from io import StringIO

from utils.db_connections import close_conn, get_conn

class Transformer:
    # ==========================================================
    # CONEXÕES
    # ==========================================================

    def get_pg_conn(self):
        print("\t- CONECTANDO AO POSTGRES")
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    # ==========================================================
    # COPY OTIMIZADO
    # ==========================================================

    def copy_dataframe(self, conn, df, table):
        # Remove null bytes (\x00) que o PostgreSQL rejeita
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        df[str_cols] = df[str_cols].apply(
            lambda col: col.str.replace("\x00", "", regex=False)
        )

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = conn.cursor()
        cursor.copy_expert(
            f"COPY {table} FROM STDIN WITH CSV",
            buffer
        )
        conn.commit()
        cursor.close()


    # ==========================================================
    # LOAD GOLD
    # ==========================================================

    def transform(self):
        print("INICIANDO CARGA GOLD")

        silver_path = os.getenv("SILVER_DATA_PATH") + "/combustiveis/**/*.parquet"

        duck, db_path = get_conn()
        pg = self.get_pg_conn()

        # LIMPA GOLD (FULL LOAD)
        print("\t- LIMPANDO TABELAS GOLD")

        cursor = pg.cursor()
        cursor.execute("""
            TRUNCATE fact_precos_combustivel,
                    dim_data,
                    dim_produto,
                    dim_localidade,
                    dim_posto
            RESTART IDENTITY CASCADE;
        """)
        pg.commit()
        cursor.close()
        
        # Lê todos Parquets
        df = duck.execute(f"""
            SELECT *
            FROM read_parquet('{silver_path}')
        """).fetchdf()

        print(f"\t- TOTAL REGISTROS SILVER: {len(df)}")

        # =============================
        # DIM_DATA
        # =============================
        dim_data = df[['data_coleta','ano','mes']].drop_duplicates()
        dim_data['trimestre'] = ((dim_data['mes'] - 1) // 3) + 1

        self.copy_dataframe(pg, dim_data, "dim_data(data,ano,mes,trimestre)")

        # =============================
        # DIM_PRODUTO
        # =============================
        dim_produto = df[['produto','unidade_medida']].drop_duplicates()
        self.copy_dataframe(pg, dim_produto, "dim_produto(produto,unidade_medida)")

        # =============================
        # DIM_LOCALIDADE
        # =============================
        dim_localidade = df[['regiao_sigla','estado_sigla','municipio']].drop_duplicates()
        self.copy_dataframe(pg, dim_localidade, "dim_localidade(regiao_sigla,estado_sigla,municipio)")

        # =============================
        # DIM_POSTO
        # =============================
        dim_posto = df[[
            'revenda','cnpj_revenda','bandeira',
            'nome_rua','numero_rua','complemento','bairro','cep'
        ]].drop_duplicates()

        self.copy_dataframe(pg, dim_posto,
            "dim_posto(revenda,cnpj_revenda,bandeira,nome_rua,numero_rua,complemento,bairro,cep)"
        )

        print("\t- DIMENSÕES CARREGADAS")

        # =============================
        # FATO
        # =============================
        print("\t- CRIANDO STAGING NO POSTGRES")

        cursor = pg.cursor()
        cursor.execute("""
        DROP TABLE IF EXISTS staging_silver;

        CREATE UNLOGGED TABLE staging_silver (
            stg_id SERIAL PRIMARY KEY,
            filename TEXT,
            row_hash TEXT,
            regiao_sigla TEXT,
            estado_sigla TEXT,
            municipio TEXT,
            revenda TEXT,
            cnpj_revenda TEXT,
            nome_rua TEXT,
            numero_rua TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            produto TEXT,
            data_coleta DATE,
            valor_venda DOUBLE PRECISION,
            valor_compra DOUBLE PRECISION,
            unidade_medida TEXT,
            bandeira TEXT,
            ano TEXT,
            mes TEXT
        );
        """)
        pg.commit()
        cursor.close()

        self.copy_dataframe(pg, df, """
            staging_silver(
                filename,
                row_hash,
                regiao_sigla,
                estado_sigla,
                municipio,
                revenda,
                cnpj_revenda,
                nome_rua,
                numero_rua,
                complemento,
                bairro,
                cep,
                produto,
                data_coleta,
                valor_venda,
                valor_compra,
                unidade_medida,
                bandeira,
                ano,
                mes
            )
        """)
        
        print("\t- PREPARANDO INSERÇÃO NA FATO")

        cursor = pg.cursor()

        # 1) Índices na staging para JOINs
        print("\t  ▸ Criando índices na staging")
        cursor.execute("""
            CREATE INDEX idx_stg_data    ON staging_silver(data_coleta);
            CREATE INDEX idx_stg_produto ON staging_silver(produto, unidade_medida);
            CREATE INDEX idx_stg_local   ON staging_silver(regiao_sigla, estado_sigla, municipio);
            CREATE INDEX idx_stg_cnpj    ON staging_silver(cnpj_revenda);
        """)
        pg.commit()

        cursor.execute("ANALYZE staging_silver;")
        pg.commit()

        # 2) Materializar dim_posto (1 posto por CNPJ) — evita refazer DISTINCT ON a cada chunk
        print("\t  ▸ Materializando lookup de postos")
        cursor.execute("""
            DROP TABLE IF EXISTS tmp_posto_lookup;
            CREATE TEMP TABLE tmp_posto_lookup AS
            SELECT DISTINCT ON (cnpj_revenda) cnpj_revenda, posto_id
            FROM dim_posto
            ORDER BY cnpj_revenda, posto_id;
            CREATE INDEX idx_tmp_posto ON tmp_posto_lookup(cnpj_revenda);
            ANALYZE tmp_posto_lookup;
        """)
        pg.commit()

        # 3) Drop indexes da fato (bulk load pattern — recria no final)
        print("\t  ▸ Removendo índices da fato (bulk load)")
        cursor.execute("""
            DROP INDEX IF EXISTS idx_fact_data_id;
            DROP INDEX IF EXISTS idx_fact_produto_id;
            DROP INDEX IF EXISTS idx_fact_localidade_id;
            DROP INDEX IF EXISTS idx_fact_posto_id;
        """)
        pg.commit()

        # 4) Conta total e insere em chunks por stg_id (sem OFFSET)
        cursor.execute("SELECT MIN(stg_id), MAX(stg_id) FROM staging_silver;")
        min_id, max_id = cursor.fetchone()
        total = max_id - min_id + 1

        CHUNK_SIZE = 500_000
        inserted = 0
        current_id = min_id

        cursor.execute("SET work_mem = '512MB';")
        pg.commit()

        print(f"\t- INSERINDO FATO ({total:,} registros, chunks de {CHUNK_SIZE:,})")

        while current_id <= max_id:
            end_id = current_id + CHUNK_SIZE - 1

            cursor.execute("""
                INSERT INTO fact_precos_combustivel (
                    data_id, produto_id, localidade_id, posto_id,
                    valor_venda, valor_compra
                )
                SELECT
                    d.data_id,
                    p.produto_id,
                    l.localidade_id,
                    po.posto_id,
                    s.valor_venda,
                    s.valor_compra
                FROM staging_silver s
                JOIN dim_data d
                    ON s.data_coleta = d.data
                JOIN dim_produto p
                    ON s.produto = p.produto
                   AND s.unidade_medida = p.unidade_medida
                JOIN dim_localidade l
                    ON s.regiao_sigla = l.regiao_sigla
                   AND s.estado_sigla = l.estado_sigla
                   AND s.municipio = l.municipio
                JOIN tmp_posto_lookup po
                    ON s.cnpj_revenda = po.cnpj_revenda
                WHERE s.stg_id BETWEEN %s AND %s;
            """, (current_id, end_id))
            pg.commit()

            rows = cursor.rowcount
            inserted += rows
            current_id = end_id + 1
            pct = min((current_id - min_id) / total * 100, 100)
            print(f"\t  ▸ {inserted:>12,} inseridos  ({pct:5.1f}%)")

        # 5) Recria índices da fato
        print("\t  ▸ Recriando índices da fato")
        cursor.execute("""
            CREATE INDEX idx_fact_data_id       ON fact_precos_combustivel(data_id);
            CREATE INDEX idx_fact_produto_id    ON fact_precos_combustivel(produto_id);
            CREATE INDEX idx_fact_localidade_id ON fact_precos_combustivel(localidade_id);
            CREATE INDEX idx_fact_posto_id      ON fact_precos_combustivel(posto_id);
        """)
        pg.commit()

        cursor.execute("ANALYZE fact_precos_combustivel;")
        pg.commit()

        cursor.execute("RESET work_mem;")
        pg.commit()
        cursor.close()

        print(f"\t- FATO CARREGADA COM SUCESSO ({inserted:,} registros)")
        
        close_conn(duck, db_path)