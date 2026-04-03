import os
import psycopg2

class SchemaCreator:
    def get_pg_conn(self):
        print("\t- CONECTANDO AO POSTGRES")
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def create_schema(self, conn):
        print("\t- CRIANDO STAR SCHEMA (GOLD)")

        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_data (
            data_id SERIAL PRIMARY KEY,
            data DATE UNIQUE,
            ano INT,
            mes INT,
            trimestre INT
        );

        CREATE TABLE IF NOT EXISTS dim_produto (
            produto_id SERIAL PRIMARY KEY,
            produto TEXT,
            unidade_medida TEXT,
            UNIQUE(produto, unidade_medida)
        );

        CREATE TABLE IF NOT EXISTS dim_localidade (
            localidade_id SERIAL PRIMARY KEY,
            regiao_sigla TEXT,
            estado_sigla TEXT,
            municipio TEXT,
            UNIQUE(regiao_sigla, estado_sigla, municipio)
        );

        -- ==========================================================
        -- DIM_POSTO (SCD TYPE 2)
        -- ==========================================================
        CREATE TABLE IF NOT EXISTS dim_posto (
            posto_id SERIAL PRIMARY KEY,
            cnpj_revenda TEXT,
            revenda TEXT,
            bandeira TEXT,
            nome_rua TEXT,
            numero_rua TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            data_inicio DATE,
            data_fim DATE,
            ativo BOOLEAN DEFAULT TRUE
        );

        CREATE INDEX IF NOT EXISTS idx_dim_posto_cnpj
        ON dim_posto(cnpj_revenda);

        CREATE TABLE IF NOT EXISTS fact_precos_combustivel (
            fato_id BIGSERIAL PRIMARY KEY,
            data_id INT REFERENCES dim_data(data_id),
            produto_id INT REFERENCES dim_produto(produto_id),
            localidade_id INT REFERENCES dim_localidade(localidade_id),
            posto_id INT REFERENCES dim_posto(posto_id),
            valor_venda DOUBLE PRECISION,
            valor_compra DOUBLE PRECISION
        );

        -- ==========================================================
        -- ÍNDICES NA TABELA FATO (performance das queries analíticas)
        -- ==========================================================
        CREATE INDEX IF NOT EXISTS idx_fact_data_id
        ON fact_precos_combustivel(data_id);

        CREATE INDEX IF NOT EXISTS idx_fact_produto_id
        ON fact_precos_combustivel(produto_id);

        CREATE INDEX IF NOT EXISTS idx_fact_localidade_id
        ON fact_precos_combustivel(localidade_id);

        CREATE INDEX IF NOT EXISTS idx_fact_posto_id
        ON fact_precos_combustivel(posto_id);
        """)

        conn.commit()
        cursor.close()


    def main(self):
        conn = self.get_pg_conn()
        self.create_schema(conn)
        conn.close()
        print("SCHEMA GOLD CRIADO COM SUCESSO\n")