# etl_sp_capital.py
import os, re, json, unicodedata
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import yaml

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))

IN_DIST   = cfg["inputs"]["distritos_geojson"]
IN_INEP   = cfg["inputs"]["inep_csv"]                 # INEP cadastral 2023 com Latitude/Longitude
IN_IDEB   = cfg["inputs"]["ideb_csv"]                 # se por escola; se não, IDEB por distrito será ignorado
IN_MAPA   = cfg["inputs"]["mapa_ods"]
MAPA_SHEET= cfg["inputs"]["mapa_sheet"]

OUT_AGG   = cfg["outputs"]["agg_parquet"]
OUT_NORM  = cfg["outputs"]["norm_json"]

# =================== helpers ===================

def norm_str(s: str) -> str:
    """Normaliza nomes para join: minúsculas, sem acento, separadores como '-'."""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))  # remove acentos
    s = s.replace("'", "").replace("`","").replace("´","")
    s = s.replace("–","-").replace("—","-")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

# mapeamentos de nomes “sem acento/variante” -> forma oficial normalizada (também sem acento)
NAME_OVERRIDES = {
    "republica": "republica",          # REPÚBLICA
    "se": "se",                        # SÉ
    "saude": "saude",                  # SAÚDE
    "agua-rasa": "agua-rasa",          # ÁGUA RASA
    "freguesia-do-o": "freguesia-do-o",# FREGUESIA DO Ó
    "vila-curuca": "vila-curuca",      # VILA CURUÇÁ
    "sao-domingos": "sao-domingos",
    "sao-lucas": "sao-lucas",
    "sao-mateus": "sao-mateus",
    "sao-miguel": "sao-miguel",
    "sao-rafael": "sao-rafael",
    # adicione aqui novos casos encontrados no log de faltantes
}

def first_col(df, candidates):
    """Encontra a primeira coluna do DF que casa com a lista de candidatos (case-insensitive)."""
    if not candidates:
        return None
    m = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in m:
            return m[cand.lower()]
    # regex leve (quando candidatos contiverem padrões)
    for c in df.columns:
        lc = c.lower()
        for cand in candidates:
            if cand.startswith("^") and re.fullmatch(cand, lc):
                return c
    return None

def normalize_rate(x):
    if pd.isna(x): return np.nan
    try:
        v = float(str(x).replace(",", "."))
    except:
        return np.nan
    return v/100.0 if v > 1 else v

def minmax(col):
    s = pd.to_numeric(col, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or np.isclose(mn, mx):
        return pd.Series([0.0]*len(s), index=s.index)
    return (s - mn) / (mx - mn)

# =================== loaders ===================

def load_distritos(path):
    """
    Lê o GeoJSON de distritos *como está* (UTM SIRGAS 2000, layer qualquer),
    define CRS de origem se estiver faltando, reprojeta para WGS84 (EPSG:4326),
    e padroniza colunas 'id' e 'name' a partir de campos oficiais.
    """
    g = gpd.read_file(path)

    # 1) Definir CRS de origem caso o arquivo não tenha (mais comum: EPSG:31983)
    if g.crs is None:
        g = g.set_crs(31983)   # <-- mude para 31984 se necessário
    g = g.to_crs(4326)

    # 2) Padronizar colunas a partir das properties
    rename_map = {}
    # nome oficial
    for cand in ["nm_distrito_municipal","NOME_DIST","Distritos","distrito","Nome","nome","NOME","name"]:
        if cand in g.columns:
            rename_map[cand] = "name"; break
    # id oficial
    for cand in ["cd_identificador_distrito","cd_distrito_municipal","CD_DIST","codigo","id_distrito","id"]:
        if cand in g.columns:
            rename_map[cand] = "id"; break
    if rename_map:
        g = g.rename(columns=rename_map)

    if "name" not in g.columns:
        g["name"] = [f"distrito_{i}" for i in range(len(g))]
        print("[distritos] AVISO: não encontrei 'nm_distrito_municipal' (ou equivalente). Usando nome sintético.")
    else:
        g["name"] = g["name"].astype(str).str.strip()

    if "id" not in g.columns:
        g["id"] = [f"distrito_{i}" for i in range(len(g))]
        print("[distritos] AVISO: não encontrei 'cd_identificador_distrito'/'cd_distrito_municipal' (ou eq.). Usando id sintético.")
    g["_norm"] = g["name"].apply(norm_str).replace(NAME_OVERRIDES)

    return g[["id","name","_norm","geometry"]]

def load_inep_cadastral(path):
    """Carrega planilha INEP cadastral 2023 com Latitude/Longitude e campos úteis."""
    df = pd.read_csv(path)
    s = cfg["schema"]["inep"]

    col_id  = first_col(df, s["id_escola"]) or "id_escola"
    col_lon = first_col(df, s["lon"])
    col_lat = first_col(df, s["lat"])
    if not all([col_id, col_lon, col_lat]):
        raise ValueError("INEP cadastral: preciso de Código INEP (id_escola) + Latitude + Longitude.")

    df = df.rename(columns={col_id: "id_escola", col_lon: "lon", col_lat: "lat"})

    # extras úteis (opcionais)
    col_rede  = first_col(df, s.get("rede", []))
    col_local = first_col(df, s.get("localizacao", []))
    col_etapa = first_col(df, s.get("etapa", []))
    col_end   = first_col(df, s.get("endereco", []))

    if col_rede and col_rede != "rede": df = df.rename(columns={col_rede: "rede"})
    if col_local and col_local != "localizacao": df = df.rename(columns={col_local: "localizacao"})
    if col_etapa and col_etapa != "etapa": df = df.rename(columns={col_etapa: "etapa"})
    if col_end and col_end != "endereco": df = df.rename(columns={col_end: "endereco"})

    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]), crs=4326)

    if "rede" in gdf.columns:
        gdf["rede"] = gdf["rede"].astype(str).str.strip().str.lower().replace({
            "municipal":"municipal", "pública municipal":"municipal", "pública/municipal":"municipal",
            "estadual":"estadual", "pública estadual":"estadual", "pública/estadual":"estadual",
            "privada":"privada", "particular":"privada"
        })

    return gdf

def load_ideb(path):
    """Se o IDEB for por escola, agregamos por distrito; se não, fica como metadado e não entra no LLM."""
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    s = cfg["schema"]["ideb"]
    col_id = first_col(df, s["id_escola"])  # pode ser None
    col_ideb = first_col(df, s["ideb"])
    col_ano  = first_col(df, s["ano"])
    if not col_ideb or not col_ano:
        print("[aviso] IDEB: não encontrei colunas ideb/ano; ignorando IDEB.")
        return None
    df = df.rename(columns={col_ideb:"ideb", col_ano:"ideb_year"})
    df["ideb"] = pd.to_numeric(df["ideb"], errors="coerce")
    df["ideb_year"] = pd.to_numeric(df["ideb_year"], errors="coerce").astype("Int64")
    if col_id:
        df = df.rename(columns={col_id:"id_escola"})
        return df[["id_escola","ideb","ideb_year"]]
    else:
        return df[["ideb","ideb_year"]].assign(id_escola=pd.NA)

def load_mapa(path, sheet):
    engine = "odf" if path.endswith(".ods") else None
    xls = pd.ExcelFile(path, engine=engine)
    if sheet not in xls.sheet_names:
        raise ValueError(f"Aba '{sheet}' não encontrada. Abas disponíveis: {xls.sheet_names}")
    df = pd.read_excel(xls, sheet_name=sheet)

    s = cfg["schema"]["mapa"]
    name_col  = first_col(df, s["distrito"]) or df.columns[0]

    # 'score' pode ser string única ou lista de colunas
    score_cfg = s["score"]
    if isinstance(score_cfg, str):
        score_cfg = [score_cfg]

    def find_col(label):
        for c in df.columns:
            if str(c).strip() == str(label).strip():
                return c
        return None

    cols = [find_col(lbl) for lbl in score_cfg]
    cols = [c for c in cols if c]
    if not cols:
        raise ValueError("Mapa 2023: nenhuma coluna em schema.mapa.score foi encontrada nessa aba.")

    out = df[[name_col] + cols].copy()
    out.columns = ["name"] + cols

    for c in cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # z-score e média (maior=pior) + normalização 0..1
    z = (out[cols] - out[cols].mean())/out[cols].std(ddof=0)
    out["indice_marginalidade_2023"] = z.mean(axis=1)
    m, M = out["indice_marginalidade_2023"].min(), out["indice_marginalidade_2023"].max()
    out["indice_marginalidade_2023"] = (out["indice_marginalidade_2023"] - m) / (M - m) if M > m else 0.0

    out["name"]  = out["name"].astype(str).str.strip()
    out["_norm"] = out["name"].apply(norm_str).replace(NAME_OVERRIDES)  # <== override aplicado aqui

    print(f"[mapa] colunas usadas p/ score: {cols}")
    return out[["_norm","indice_marginalidade_2023"]]

# =================== ETL ===================

os.makedirs("out", exist_ok=True)

# 1) Geo distritos
gdist = load_distritos(IN_DIST)

# 2) INEP cadastral 2023 (com lat/lon)
inep = load_inep_cadastral(IN_INEP)

# Limpa coordenadas inválidas / nulas
inep = inep.dropna(subset=["lon","lat"])
inep = inep[inep["lon"].between(-180, 180) & inep["lat"].between(-90, 90)]

# 3) Spatial join (defensivo)
joined = gpd.sjoin(
    inep,
    gdist[["id","name","_norm","geometry"]].copy(),
    predicate="intersects",
    how="left"
).rename(columns={"id":"bairro_id","name":"bairro_name","_norm":"_norm_name"})

# remove colunas duplicadas
joined = joined.loc[:, ~joined.columns.duplicated(keep="last")]
if (joined.columns == "bairro_id").sum() > 1:
    joined["bairro_id"] = joined.loc[:, "bairro_id"].iloc[:, -1]
    mask = (joined.columns.to_series() == "bairro_id") & joined.columns.to_series().duplicated(keep="last")
    joined = joined.loc[:, ~mask]
if (joined.columns == "bairro_name").sum() > 1:
    joined["bairro_name"] = joined.loc[:, "bairro_name"].iloc[:, -1]
    mask = (joined.columns.to_series() == "bairro_name") & joined.columns.to_series().duplicated(keep="last")
    joined = joined.loc[:, ~mask]
if isinstance(joined.get("bairro_id"), pd.DataFrame):
    joined["bairro_id"] = joined["bairro_id"].iloc[:, -1]
if isinstance(joined.get("bairro_name"), pd.DataFrame):
    joined["bairro_name"] = joined["bairro_name"].iloc[:, -1]

rate = float(joined["bairro_id"].notna().mean())
print(f"[join] escolas atribuídas a distrito: {rate:.1%}")

# 4) IDEB (se por escola)
ideb_df = load_ideb(IN_IDEB)

# 5) Agregação por distrito
def flag_rede(s, chave):
    s = s.astype(str).str.lower().fillna("")
    return s.str.contains(chave, regex=False)

joined["is_municipal"] = flag_rede(joined.get("rede",""), "municipal")
joined["is_estadual"]  = flag_rede(joined.get("rede",""), "estadual")
joined["is_privada"]   = flag_rede(joined.get("rede",""), "privada")

joined["is_ei_creche"] = joined.get("etapa","").astype(str).str.upper().str.contains("CRECHE|INFANTIL|EDUCAÇÃO INFANTIL", regex=True, na=False)

grp = joined.groupby(["bairro_id","bairro_name"], dropna=False)
agg = grp.apply(lambda g: pd.Series({
    "schools_total": int(g.shape[0]),
    "schools_municipal": int(g["is_municipal"].sum()),
    "schools_estadual": int(g["is_estadual"].sum()),
    "schools_privada": int(g["is_privada"].sum()),
    "acesso_creche_proxy": (g["is_ei_creche"].mean() if g.shape[0]>0 else np.nan)
}), include_groups=False).reset_index()

# Remove linhas sem distrito (se houver)
sem_distrito = agg["bairro_id"].isna().sum()
if sem_distrito:
    print(f"[join] Removendo {sem_distrito} linhas sem distrito (pontos fora/coords inválidas)")
    agg = agg[agg["bairro_id"].notna()].copy()

# IDEB por escola -> média por distrito (se existir)
if ideb_df is not None and "id_escola" in ideb_df.columns and ideb_df["id_escola"].notna().any():
    df = joined.merge(ideb_df[["id_escola","ideb","ideb_year"]], on="id_escola", how="left")
    ideb_agg = df.groupby(["bairro_id","bairro_name"], dropna=False).apply(lambda g: pd.Series({
        "ideb": np.nanmean(g["ideb"]),
        "ideb_year": g["ideb_year"].dropna().max() if g["ideb_year"].notna().any() else pd.NA
    }), include_groups=False).reset_index()
    agg = agg.merge(ideb_agg, on=["bairro_id","bairro_name"], how="left")
else:
    agg["ideb"] = np.nan
    agg["ideb_year"] = pd.NA

# 6) Mapa da Desigualdade 2023 (join por nome normalizado + overrides)
mapa = load_mapa(IN_MAPA, MAPA_SHEET)   # -> [_norm, indice_marginalidade_2023]
agg["_norm"] = agg["bairro_name"].apply(norm_str).replace(NAME_OVERRIDES)  # <== override aplicado aqui também
agg = agg.merge(mapa, on="_norm", how="left").drop(columns=["_norm"])

cov = agg["indice_marginalidade_2023"].notna().mean()
print(f"[mapa] distritos com marginalidade preenchida: {cov:.1%}")
if cov < 0.98:
    falt = (agg.loc[agg["indice_marginalidade_2023"].isna(),"bairro_name"]
              .dropna().unique().tolist()[:20])
    print("[mapa] Exemplos sem match (adicione em NAME_OVERRIDES se necessário):", falt)

# 7) Salva agregado determinístico
agg.to_parquet(OUT_AGG, engine="fastparquet", index=False)
print(f"[ok] agregado: {OUT_AGG}")

# 8) Normalização para o LLM (somente indicadores disponíveis)
N = pd.DataFrame(index=agg.index)
N["marginalidade"]       = minmax(agg["indice_marginalidade_2023"])
N["schools_total_bad"]   = 1 - minmax(agg["schools_total"])
N["share_municipal_bad"] = 1 - minmax(agg["schools_municipal"] / agg["schools_total"].replace(0, np.nan))
N["share_estadual_bad"]  = 1 - minmax(agg["schools_estadual"]  / agg["schools_total"].replace(0, np.nan))
N["acesso_creche_bad"]   = 1 - minmax(agg["acesso_creche_proxy"])

# SOMENTE SE TIVER IDEB por distrito (agregado de escolas):
if agg["ideb"].notna().any():
    N["ideb_good"] = minmax(agg["ideb"])

# >>> FILL NEUTRO (evitar NaN -> 0)
# marginalidade faltante = 0.5 (neutro); demais = média da coluna
if "marginalidade" in N.columns:
    N["marginalidade"] = N["marginalidade"].fillna(0.5)
for col in ["schools_total_bad","share_municipal_bad","share_estadual_bad","acesso_creche_bad","ideb_good"]:
    if col in N.columns:
        N[col] = N[col].fillna(N[col].mean())

# (remover qualquer resto de NaN que sobrar)
N = N.fillna(0.5)

items = []
for i, r in agg.reset_index(drop=True).iterrows():
    norm = {
        "marginalidade": float(N.loc[i, "marginalidade"]),
        "schools_total_bad": float(N.loc[i, "schools_total_bad"]),
        "share_municipal_bad": float(N.loc[i, "share_municipal_bad"]),
        "share_estadual_bad": float(N.loc[i, "share_estadual_bad"]),
        "acesso_creche_bad": float(N.loc[i, "acesso_creche_bad"])
    }
    if "ideb_good" in N.columns:
        norm["ideb_good"] = float(N.loc[i, "ideb_good"])

    items.append({"id": str(r["bairro_id"]), "name": r["bairro_name"], "norm": norm})

with open(OUT_NORM, "w", encoding="utf-8") as f:
    json.dump({"distritos": items}, f, ensure_ascii=False, indent=2)

print(f"[ok] normalizado p/ LLM: {OUT_NORM}")
