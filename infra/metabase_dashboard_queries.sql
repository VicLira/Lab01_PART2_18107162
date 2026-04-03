-- =============================================================================
-- QUERIES ANALÍTICAS - DASHBOARD COMBUSTÍVEIS (CAMADA GOLD)
-- Conectar o Metabase ao PostgreSQL (combustiveis_dw) e criar uma
-- "Question" do tipo "Native query" para cada visualização abaixo.
-- =============================================================================


-- =============================================================================
-- 1) EVOLUÇÃO DO PREÇO MÉDIO POR COMBUSTÍVEL AO LONGO DO TEMPO
--    Tipo: Gráfico de Linhas  |  Eixo X: mês/ano  |  Eixo Y: preço médio
--    Insight: tendência de alta/baixa de cada combustível ao longo dos anos
-- =============================================================================
SELECT
    d.ano,
    d.mes,
    p.produto,
    ROUND(AVG(f.valor_venda)::numeric, 3)  AS preco_medio_venda,
    ROUND(AVG(f.valor_compra)::numeric, 3) AS preco_medio_compra
FROM fact_precos_combustivel f
JOIN dim_data     d  ON f.data_id      = d.data_id
JOIN dim_produto  p  ON f.produto_id   = p.produto_id
GROUP BY d.ano, d.mes, p.produto
ORDER BY d.ano, d.mes, p.produto;


-- =============================================================================
-- 2) PREÇO MÉDIO POR ESTADO (MAPA / BARRAS HORIZONTAIS)
--    Tipo: Mapa coroplético ou Barras Horizontais
--    Insight: quais estados têm combustível mais caro/barato
-- =============================================================================
SELECT
    l.estado_sigla,
    p.produto,
    ROUND(AVG(f.valor_venda)::numeric, 3)  AS preco_medio_venda,
    COUNT(*)                                AS total_coletas
FROM fact_precos_combustivel f
JOIN dim_localidade l ON f.localidade_id = l.localidade_id
JOIN dim_produto   p  ON f.produto_id    = p.produto_id
GROUP BY l.estado_sigla, p.produto
ORDER BY l.estado_sigla, p.produto;


-- =============================================================================
-- 3) RANKING TOP 10 MUNICÍPIOS MAIS CAROS (GASOLINA)
--    Tipo: Gráfico de Barras Horizontais
--    Insight: municípios onde o consumidor paga mais caro
-- =============================================================================
SELECT
    l.municipio,
    l.estado_sigla,
    ROUND(AVG(f.valor_venda)::numeric, 3) AS preco_medio,
    COUNT(*)                               AS total_coletas
FROM fact_precos_combustivel f
JOIN dim_localidade l ON f.localidade_id = l.localidade_id
JOIN dim_produto   p  ON f.produto_id    = p.produto_id
WHERE p.produto = 'GASOLINA'
GROUP BY l.municipio, l.estado_sigla
HAVING COUNT(*) >= 100          -- mínimo de coletas para relevância
ORDER BY preco_medio DESC
LIMIT 10;


-- =============================================================================
-- 4) MARGEM MÉDIA (VENDA - COMPRA) POR REGIÃO E COMBUSTÍVEL
--    Tipo: Gráfico de Barras Agrupadas (região x margem, cor = combustível)
--    Insight: onde a margem do posto é maior/menor
-- =============================================================================
SELECT
    l.regiao_sigla,
    p.produto,
    ROUND(AVG(f.valor_venda - f.valor_compra)::numeric, 3) AS margem_media,
    ROUND(AVG(f.valor_venda)::numeric, 3)                  AS preco_venda_medio,
    ROUND(AVG(f.valor_compra)::numeric, 3)                 AS preco_compra_medio
FROM fact_precos_combustivel f
JOIN dim_localidade l ON f.localidade_id = l.localidade_id
JOIN dim_produto   p  ON f.produto_id    = p.produto_id
WHERE f.valor_compra IS NOT NULL
  AND f.valor_venda  IS NOT NULL
GROUP BY l.regiao_sigla, p.produto
ORDER BY l.regiao_sigla, margem_media DESC;


-- =============================================================================
-- 5) DISTRIBUIÇÃO DE BANDEIRAS (MARKET SHARE POR Nº DE COLETAS)
--    Tipo: Gráfico de Pizza / Donut
--    Insight: concentração de mercado por bandeira
-- =============================================================================
SELECT
    COALESCE(NULLIF(po.bandeira, ''), 'BRANCA') AS bandeira,
    COUNT(*)                                      AS total_coletas,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentual
FROM fact_precos_combustivel f
JOIN dim_posto po ON f.posto_id = po.posto_id
GROUP BY po.bandeira
ORDER BY total_coletas DESC
LIMIT 15;


-- =============================================================================
-- 6) VARIAÇÃO TRIMESTRAL DO PREÇO MÉDIO (GASOLINA vs ETANOL)
--    Tipo: Gráfico de Linhas Duplo
--    Insight: sazonalidade e competição gasolina × etanol
-- =============================================================================
SELECT
    d.ano,
    d.trimestre,
    p.produto,
    ROUND(AVG(f.valor_venda)::numeric, 3) AS preco_medio
FROM fact_precos_combustivel f
JOIN dim_data    d ON f.data_id    = d.data_id
JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE p.produto IN ('GASOLINA', 'ETANOL')
GROUP BY d.ano, d.trimestre, p.produto
ORDER BY d.ano, d.trimestre, p.produto;


-- =============================================================================
-- 7) DISPERSÃO: PREÇO DE VENDA vs PREÇO DE COMPRA (SCATTER PLOT)
--    Tipo: Gráfico de Dispersão (scatter)
--    Insight: correlação compra/venda — pontos acima da diagonal = margem alta
--    NOTA: usar TABLESAMPLE ou LIMIT para performance no Metabase
-- =============================================================================
SELECT
    p.produto,
    f.valor_compra,
    f.valor_venda
FROM fact_precos_combustivel f
JOIN dim_produto p ON f.produto_id = p.produto_id
WHERE f.valor_compra IS NOT NULL
  AND f.valor_venda  IS NOT NULL
  AND f.valor_compra > 0
  AND f.valor_venda  > 0
ORDER BY RANDOM()
LIMIT 5000;
