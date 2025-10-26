# Lumen — Backend de Indicadores, Normalização e GeoJSON para Front

Este repositório gera, versiona e publica um **GeoJSON otimizado** de distritos de São Paulo com **indicadores normalizados**, um **índice de marginalidade composto** e um **ranking explicável** produzido por LLM — pronto para consumo no front (com Brotli/CDN).

## Arquitetura / Fluxo

1. **`etl_sp_capital.py`**  
   - Lê o **GeoJSON de distritos** (projeta para WGS84 se necessário, corrige CRS ausente).
   - Lê **INEP cadastral 2023** (com *Latitude/Longitude*), limpa coordenadas inválidas e faz *spatial join* com distritos.
   - Agrega por distrito: total de escolas, por rede, proxy de **acesso a creche** via etapas.
   - Lê **Mapa da Desigualdade 2023** (ODS), **normaliza e pondera indicadores** (com inversão para positivos).
   - Opcionalmente agrega **IDEB por distrito** caso exista por escola.
   - Salva:
     - `out/agg.parquet` (agregado determinístico)
     - `out/norm_for_llm.json` (features normalizadas para LLM)

2. **`rank_llm.py`**  
   - Consome `norm_for_llm.json`.
   - Pergunta ao LLM (via `config.llm.url`) um **ranking com justificativa e “drivers”**.
   - Salva `out/llm_ranking.json`.

3. **`build_featurecollection.py`**  
   - Junta geometrias + indicadores + ranking no **GeoJSON final** para o front:
     - `out/distritos_front.geojson` (minificado, com *properties* ricas).
   - (Opcional) Gera versão `.geojson.br` (Brotli).

---

## Entradas (dados brutos)

Definidas no `config.yaml`:

- `data/geojson/sao_paulo_distritos.geojson` — Distritos de SP.
- `data/raw/inep_escolas_2023_sp.csv` — INEP cadastral (2023) com lon/lat e metadados básicos.
- `data/raw/ideb_escolas_2023_sp.csv` — IDEB por escola (opcional).
- `data/raw/mapa_desigualdade_2023.ods` — Aba `2__Dados_distritos_2023`.

> **Observação:** o ETL é resiliente a variações de nomes de colunas (ver [Configuração](#configuração-configyaml)).

---

## Saídas (artefatos)

- **`out/agg.parquet`** — tabela agregada por distrito (determinístico).
- **`out/norm_for_llm.json`** — normalizações 0..1 para o LLM por distrito.
- **`out/llm_ranking.json`** — ranking com `rank`, `llm_score`, `tier` e `drivers`.
- **`out/distritos_front.geojson`** — FeatureCollection final (pronto para front).
  - **Minificado** e com *properties* enxutas (sem `null`).
  - Pode ter versão `*.geojson.br` (Brotli) para CDN.

---

## Instalação

> Recomendado Python 3.10+.

```bash
# 1) Clone
git clone <repo> sp-bairros
cd sp-bairros

# 2) Ambiente
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)

# 3) Dependências
pip install -r requirements.txt
```
# (Geo stack: geopandas, pyproj, shapely, fiona. Caso falhe, use conda/mamba.)
Configuração (config.yaml)
Schema de colunas para tornar o ETL tolerante a planilhas heterogêneas.

Controle de pesos e inversão de indicadores (positivos → inverter).

Exemplo mínimo para Mapa 2023:

```yaml
schema:
  mapa:
    distrito: ["Distritos","distrito","nome_distrito","name"]
    score:
      - "Favelas"
      - "População em situação de rua"
      - "Homicídios"
      - "Abandono escolar no ensino fundamental da rede municipal"
      - "Distorção idade-série no ensino fundamental da rede municipal"
      - "Acesso a transporte de massa"                      # positivo (inverter)
      - "Acesso internet móvel (População distrito)"        # positivo (inverter)

    positive_indicators:
      - "Acesso a transporte de massa"
      - "Acesso internet móvel (População distrito)"

    weights:
      "Favelas": 1.2
      "População em situação de rua": 1.2
      "Homicídios": 1.2
      "Abandono escolar no ensino fundamental da rede municipal": 1.0
      "Distorção idade-série no ensino fundamental da rede municipal": 1.0
      "Acesso a transporte de massa": 1.0
      "Acesso internet móvel (População distrito)": 1.0

    # (Opcional) Temas: soma de pesos por tema + indicador
    themes:
      habitacao:
        weight: 1.2
        items: ["Favelas","População em situação de rua"]
      violencia:
        weight: 1.2
        items: ["Homicídios"]
      educacao:
        weight: 1.0
        items:
          - "Abandono escolar no ensino fundamental da rede municipal"
          - "Distorção idade-série no ensino fundamental da rede municipal"
```
Como ajustar pesos: edite weights: e/ou themes:; o ETL recalcula automaticamente o índice composto. Indicadores em positive_indicators são invertidos (1 − valor) antes de ponderar.

Execução
```bash
# 1) ETL: agrega, normaliza e calcula índice composto
python etl_sp_capital.py

# 2) Ranking LLM: gera ranking e drivers (requer LLM local ou HTTP)
python rank_llm.py

# 3) GeoJSON final para o front
python build_featurecollection.py
```

## Logs importantes:
```bash
[join] escolas atribuídas a distrito: XX%

[mapa] colunas usadas p/ score (com pesos): {...}

[mapa] distritos com marginalidade preenchida: YY%

[ok] agregado: out/agg.parquet

[ok] normalizado p/ LLM: out/norm_for_llm.json

[ok] ranking LLM: out/llm_ranking.json

[ok] geojson final: out/distritos_front.geojson
```

## Metodologia de Score
### Normalização 0..1
Cada indicador do Mapa 2023 listado em ``` schema.mapa.score ``` é convertido para 0..1 com ``` min-max ```:

```arduino
x_norm = (x − min) / (max − min)
``` 
- Se a coluna for constante (sem variação), atribuímos 0.5 (neutro).

- ```NaN``` são tratados depois com neutro (0.5) durante a ponderação.

## Indicadores “positivos” (inversão)
Para indicadores em que maior é melhor (ex.: Acesso a transporte de massa), aplicamos:

```ini
x_neg = 1 − x_norm
```
Assim, em todos os indicadores do score: maior = pior.

## Pesos por indicador e por tema
- Peso por indicador: ```schema.mapa.weights```
- Peso por tema: ```schema.mapa.themes```, somado ao peso do indicador (por padrão; pode trocar para produto no código se desejar).
- Clamp mínimo: se algum peso ≤ 0, usamos 0.0001.

## Fórmula do índice composto
1. Após normalizar (e inverter positivos), computamos uma média ponderada por distrito:

```
score_raw(d) = Σ_i [w_i * x_i(d)] / Σ_i [w_i]
```
com ```x_i(d) ∈ [0,1]```. Faltantes usam ```0.5``` (neutro).

2. Opcionalmente re-normalizamos o score final para 0..1:

```ini
indice_marginalidade_2023 = minmax(score_raw)
```
> Interpretação: mais próximo de 1 ⇒ pior na composição escolhida; mais próximo de 0 ⇒ melhor.

## Indicadores derivados para o front
A partir de agregados do INEP por distrito:

- ```schools_total_bad``` = ```1 − minmax(total de escolas)```
- ```share_municipal_bad``` = ```1 − minmax(sch_municipal / total)```
- ```share_estadual_bad``` = ```1 − minmax(sch_estadual / total)```
- ```acesso_creche_bad``` = ```1 − minmax(proxy EI/creche)```

> Estes são usados para explicabilidade no front e também enviados ao LLM para o ranking.

## Ranking por LLM
- rank_llm.py lê out/norm_for_llm.json e chama o modelo em llm.url (ex.: http://localhost:11434/api/chat com llama3.1:8b).

- Saída: out/llm_ranking.json com, por distrito:

- rank, llm_score (0..1), tier (“muito alto”, “alto”, “médio”, “baixo”, “muito baixo”)

- drivers: top 2–3 indicadores que “puxam” o score (usando nossos normalizados).

- O GeoJSON final incorpora llm_score, rank_sp, tier e drivers em properties.

> Dica de coerência: se notar distritos sabidamente vulneráveis muito bem colocados, ajuste pesos e positivos no config.yaml. Em especial, dê mais peso a Favelas, População em situação de rua, Homicídios e indicadores educacionais negativos.

## Estrutura dos arquivos de saída
```out/norm_for_llm.json``` (resumo)

```json
{
  "distritos": [
    {
      "id": "8583462",
      "name": "CIDADE ADEMAR",
      "norm": {
        "marginalidade": 0.4893,
        "schools_total_bad": 0.5357,
        "share_municipal_bad": 0.5483,
        "share_estadual_bad": 0.4594,
        "acesso_creche_bad": 0.4211,
        "ideb_good": 0.42           // opcional
      }
    }
  ]
}
```

```out/distritos_front.geojson``` (por Feature)
```json
{
  "type": "Feature",
  "geometry": { "type": "Polygon", "coordinates": [...] },
  "properties": {
    "id": "8583462",
    "name": "CIDADE ADEMAR",
    "level": "distrito",
    "uf": "SP",

    // indicadores principais (0..1, maior = pior)
    "marginalidade": 0.4893,
    "schools_total_bad": 0.5357,
    "share_municipal_bad": 0.5483,
    "share_estadual_bad": 0.4594,
    "acesso_creche_bad": 0.4211,

    // ranking
    "llm_score": 0.4893,
    "rank_sp": 58,
    "tier": "médio",
    "drivers": [
      { "name": "share_municipal_bad", "direction": "up", "contribution": 0.5483 },
      { "name": "schools_total_bad",   "direction": "up", "contribution": 0.5357 },
      { "name": "marginalidade",       "direction": "up", "contribution": 0.4817 }
    ]
  }
}
```
## Serviço/Entrega (Minify + Brotli + CDN)
- O pipeline pode gerar out/distritos_front.geojson e out/distritos_front.geojson.br (Brotli).

- Headers recomendados no backend/CDN:

  - Content-Type: application/geo+json; charset=utf-8

  - Content-Encoding: br (quando servir .br)

  - Cache-Control: public, max-age=86400, stale-while-revalidate=604800

  - ETag baseado no hash do conteúdo (para cache do front)

- CDN (ex.: Cloudflare/Akamai): habilite Brotli e cache por ETag.

- Front: use streaming fetch + loading state para a primeira carga; demais hits pegam do cache HTTP.

## Validações e correlações
### Sanidade básica (distribuições)
- Conferir min/max e quantis dos indicadores e do indice_marginalidade_2023.

- Checar % de distritos com marginalidade preenchida (log: [mapa] distritos com marginalidade ...).

- Ver exemplos sem match (log: [mapa] Exemplos sem match: [...]) e ajustar overrides.

## Correlações (exemplo de script)
> Atenção: correlação ≠ causalidade. Use Spearman para robustez a não-linearidades.

```python
import pandas as pd
from scipy.stats import spearmanr

agg = pd.read_parquet("out/agg.parquet")
norm = pd.read_json("out/norm_for_llm.json")["distritos"].explode().apply(pd.Series)
norm = pd.concat([norm.drop("norm",axis=1), pd.json_normalize(norm["norm"])], axis=1)

df = agg.merge(norm, left_on="bairro_id", right_on="id", how="inner")

cols = ["indice_marginalidade_2023","marginalidade",
        "schools_total_bad","share_municipal_bad","share_estadual_bad","acesso_creche_bad"]
for c in cols:
    r,_ = spearmanr(df["indice_marginalidade_2023"], df[c], nan_policy="omit")
    print(f"Spearman(indice_marginalidade_2023, {c}) = {r:.3f}")
```
### Leituras esperadas (sinais):

- Favelas, População em situação de rua, Homicídios, Abandono, Distorção ⇒ correlação positiva com o índice.

- Itens invertidos (ex.: Acesso a transporte de massa) já virão “maior=pior”.

## Mapeamentos de nomes e CRS
- CRS: se o GeoJSON de distritos vier sem CRS, assumimos EPSG:31983 e reprojetamos para EPSG:4326 (WGS84).

  - Se notar offset espacial, teste 31984.

- Nome do distrito: normalizado com norm_str (minúsculas, sem acento, hífens).

- Overrides de nomes: o ETL suporta um dicionário NAME_OVERRIDES para casos específicos
(ex.: “SÉ” ⇄ “SE”; “VILA CURUÇÁ” ⇄ “VILA CURUCA”).

## Resolução de problemas comuns
- Cobertura baixa do Mapa ([mapa] distritos com marginalidade preenchida: 74%)
→ Ajuste NAME_OVERRIDES e confira schema.mapa.distrito/score (rótulos exatos).

- Coordenadas inválidas INEP
→ O ETL descarta lon/lat fora de [-180..180]/[-90..90] e linhas sem distrito.

- Ranking incoerente
→ Reforce pesos de Favelas, População em situação de rua, Homicídios, Abandono, Distorção.
→ Garanta que positivos (ex.: Acesso a transporte) estão em positive_indicators.

- LLM indisponível
→ Faça fallback (opcional) para um ranking determinístico (ex.: ordenar por marginalidade).

### Anexo — Dicionário (principais properties)
| Campo                          | Descrição                                                            | Escala   |
| ------------------------------ | -------------------------------------------------------------------- | -------- |
| `id`, `name`, `uf`, `level`    | Identificação do distrito                                            | —        |
| `marginalidade`                | Índice composto (0..1, maior=pior)                                   | 0..1     |
| `schools_total_bad`            | 1−minmax(total de escolas)                                           | 0..1     |
| `share_municipal_bad`          | 1−minmax(% escolas municipais)                                       | 0..1     |
| `share_estadual_bad`           | 1−minmax(% escolas estaduais)                                        | 0..1     |
| `acesso_creche_bad`            | 1−minmax(proxy de creche por etapa)                                  | 0..1     |
| `llm_score`, `rank_sp`, `tier` | Saída do ranking por LLM                                             | 0..1 / # |
| `drivers[]`                    | Principais fatores que “puxam” o score (nome, direção, contribuição) | —        |


# Rodar servidor (local):

```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```
