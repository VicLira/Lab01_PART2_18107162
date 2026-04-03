
import os
import shutil

import great_expectations as gx
import great_expectations.expectations as gxe


# =============================
# COLUNAS ESPERADAS NA CAMADA RAW (BRONZE)
# =============================
EXPECTED_COLUMNS = [
    "Regiao - Sigla",
    "Estado - Sigla",
    "Municipio",
    "Revenda",
    "CNPJ da Revenda",
    "Nome da Rua",
    "Numero Rua",
    "Complemento",
    "Bairro",
    "Cep",
    "Produto",
    "Data da Coleta",
    "Valor de Venda",
    "Valor de Compra",
    "Unidade de Medida",
    "Bandeira",
]

PRODUTOS_VALIDOS = [
    "GASOLINA ADITIVADA"
    ,"ETANOL"
    ,"GASOLINA"
    ,"DIESEL"
    ,"GNV"
    ,"DIESEL S10"
    ,"DIESEL S50"
]

UF_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]


class Validator:
    """
    Camada de qualidade de dados (Great Expectations)
    aplicada sobre os CSVs da camada Raw/Bronze.
    """

    def __init__(self, gx_root: str | None = None):
        self._gx_root = gx_root or os.path.abspath(
            os.path.join("data", "gx")
        )

    # =============================
    # CONTEXTO DE DADOS
    # =============================
    def _get_context(self) -> gx.data_context.FileDataContext:
        """
        Cria (ou recria) um FileDataContext conectado ao filesystem.
        """
        if os.path.exists(self._gx_root):
            shutil.rmtree(self._gx_root)

        return gx.get_context(mode="file", project_root_dir=self._gx_root)

    # =============================
    # DATA SOURCE + ASSET
    # =============================
    def _add_datasource(self, ctx, bronze_path: str):
        """
        Registra o diretório bronze como PandasFilesystem DataSource.
        """
        ds = ctx.data_sources.add_pandas_filesystem(
            name="bronze_filesystem",
            base_directory=bronze_path,
        )

        asset = ds.add_csv_asset(
            name="combustiveis_raw",
            sep=";",
            encoding="utf-8-sig",
            header=0,
        )

        return asset

    # =============================
    # EXPECTATION SUITE
    # =============================
    def _build_suite(self, ctx) -> gx.ExpectationSuite:
        """
        Cria a suite com no mínimo 5 expectativas distintas
        cobrindo completude, formato, domínio e range dos dados.
        """
        suite = gx.ExpectationSuite(name="bronze_quality_suite")
        suite = ctx.suites.add(suite)

        # 1) Coluna obrigatória não-nula: Estado
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column="Estado - Sigla")
        )

        # 2) Coluna obrigatória não-nula: Produto
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(column="Produto")
        )

        # 3) Formato de data dd/mm/yyyy na coleta
        suite.add_expectation(
            gxe.ExpectColumnValuesToMatchStrftimeFormat(
                column="Data da Coleta",
                strftime_format="%d/%m/%Y",
            )
        )

        # 4) UF pertence ao conjunto válido de siglas brasileiras
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="Estado - Sigla",
                value_set=UF_VALIDAS,
            )
        )

        # 5) Unidade de medida no domínio esperado
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="Unidade de Medida",
                value_set=["R$ / m³", "R$ / m3", "R$ / litro"],
            )
        )

        # 6) Existência de coluna essencial (schema check)
        suite.add_expectation(
            gxe.ExpectColumnToExist(column="CNPJ da Revenda")
        )

        # 7) Região dentro das siglas esperadas
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeInSet(
                column="Regiao - Sigla",
                value_set=["S", "SE", "CO", "NE", "N"],
            )
        )

        return suite

    # =============================
    # VALIDAÇÃO + DATA DOCS
    # =============================
    def _run_checkpoint(self, ctx, asset, suite):
        """
        Cria batch definition, validation definition e checkpoint.
        Executa e gera os Data Docs (HTML).
        """
        batch_def = asset.add_batch_definition_path(
            name="sample_batch",
            path="ca-2004-01.csv",
        )

        vd = gx.ValidationDefinition(
            name="bronze_validation",
            data=batch_def,
            suite=suite,
        )
        vd = ctx.validation_definitions.add(vd)

        checkpoint = gx.Checkpoint(
            name="bronze_checkpoint",
            validation_definitions=[vd],
        )
        checkpoint = ctx.checkpoints.add(checkpoint)

        result = checkpoint.run()
        return result

    def _build_data_docs(self, ctx):
        """
        Gera o relatório HTML de validação (Data Docs).
        """
        ctx.build_data_docs()
        urls = ctx.get_docs_sites_urls()
        return urls

    # =============================
    # FUNÇÃO PRINCIPAL
    # =============================
    def validate(self) -> bool:
        """
        Executa o pipeline completo de qualidade:
        Contexto → DataSource → Suite → Checkpoint → Data Docs
        """
        bronze_path = os.path.abspath(os.getenv("BRONZE_DATA_PATH", "data/bronze"))

        print("\t- CONFIGURANDO CONTEXTO GREAT EXPECTATIONS")
        ctx = self._get_context()

        print("\t- REGISTRANDO DATASOURCE (BRONZE FILESYSTEM)")
        asset = self._add_datasource(ctx, bronze_path)

        print("\t- CRIANDO EXPECTATION SUITE (7 EXPECTATIONS)")
        suite = self._build_suite(ctx)

        print("\t- EXECUTANDO CHECKPOINT DE VALIDAÇÃO")
        result = self._run_checkpoint(ctx, asset, suite)

        print("\t- GERANDO DATA DOCS (HTML)")
        urls = self._build_data_docs(ctx)

        success = result.success
        status = "✅ PASSOU" if success else "⚠️  FALHOU (verifique Data Docs)"

        print(f"\t- RESULTADO DA VALIDAÇÃO: {status}")
        for url_info in urls:
            print(f"\t- DATA DOCS: {url_info['site_url']}")

        return success
