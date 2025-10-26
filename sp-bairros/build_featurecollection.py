import re, json, yaml
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import mapping

cfg = yaml.safe_load(open("config.yaml","r", encoding="utf-8"))
IN_DIST   = cfg["inputs"]["distritos_geojson"]
OUT_NORM  = cfg["outputs"]["norm_json"]         # out/norm_for_llm.json
OUT_RANK  = cfg["outputs"]["rank_json"]         # out/llm_ranking.json
OUT_AGG   = cfg.get("outputs", {}).get("agg_parquet")  # opcional (p/ ngc)
OUT_FC    = cfg["outputs"]["final_geojson"]

# ---------- utils ----------
def clean_id(v: object) -> str:
    s = str(v)
    s = re.sub(r"\.0+$", "", s)       # remove sufixo .0
    s = re.sub(r"\s+", "", s).strip()
    return s

def extract_digits(s: str) -> str:
    """Extrai os dígitos (último bloco) de um id com prefixo ex.: 'distrito_municipal_v2.8583462' -> '8583462'."""
    m = re.search(r'(\d+)\s*$', str(s))
    return m.group(1) if m else clean_id(s)

def load_distritos(fp: str) -> gpd.GeoDataFrame:
    g = gpd.read_file(fp)
    if g.crs is None:
        g = g.set_crs(31983)  # SIRGAS 2000 / UTM 23S (troque p/ 31984 se necessário)
    try:
        g = g.to_crs(4326)
    except Exception:
        pass

    cols_low = {c.lower(): c for c in g.columns}

    # name (prioriza nm_distrito_municipal)
    if "nm_distrito_municipal" in cols_low:
        g["name"] = g[cols_low["nm_distrito_municipal"]].astype(str).str.strip()
    elif "name" in g.columns:
        g["name"] = g["name"].astype(str).str.strip()
    else:
        for cand in ["nome_dist","distritos","distrito","nome","nome_distrito"]:
            if cand in cols_low:
                g["name"] = g[cols_low[cand]].astype(str).str.strip()
                break
        if "name" not in g.columns:
            g["name"] = [f"distrito_{i}" for i in range(len(g))]

    # id (prioriza cd_identificador_distrito, depois cd_distrito_municipal)
    if "cd_identificador_distrito" in cols_low:
        g["id"] = g[cols_low["cd_identificador_distrito"]].apply(clean_id)
    elif "cd_distrito_municipal" in cols_low:
        g["id"] = g[cols_low["cd_distrito_municipal"]].apply(clean_id)
    else:
        # se já existir 'id' com prefixo, extrai os dígitos
        if "id" in g.columns:
            g["id"] = g["id"].apply(extract_digits)
        else:
            # tenta última alternativa: extrair dígitos de qualquer coluna típica
            base_id = None
            for cand in ["codigo","id_distrito"]:
                if cand in cols_low:
                    base_id = cols_low[cand]; break
            if base_id:
                g["id"] = g[base_id].apply(clean_id)
            else:
                g["id"] = [f"{i}" for i in range(len(g))]

    # normaliza id
    g["id"] = g["id"].astype(str).apply(extract_digits)

    return g[["id","name","geometry"]].copy()

# ---------- carregar distritos ----------
g = load_distritos(IN_DIST)

# ---------- carregar NORM ----------
norm_payload = json.load(open(OUT_NORM, "r", encoding="utf-8"))
norm_by_id = {}
for it in norm_payload.get("distritos", []):
    nid_raw = it.get("id")
    nid = extract_digits(clean_id(nid_raw))
    norm_by_id[nid] = it.get("norm", {})

N_KEYS = ["marginalidade","schools_total_bad","share_municipal_bad","share_estadual_bad","acesso_creche_bad"]

# ---------- carregar RANK ----------
rank_payload = json.load(open(OUT_RANK,"r",encoding="utf-8"))
rank_by_id = {}
for r in rank_payload.get("ranking", []):
    rid = extract_digits(clean_id(r.get("id")))
    rank_by_id[rid] = {
        "llm_score": r.get("llm_score", None),
        "rank_sp":   r.get("rank", None),
        "tier":      r.get("tier", None),
        "drivers":   r.get("drivers", None),
        "explanation": r.get("explanation", None),
    }

# ---------- (opcional) ngc ----------
ngc_by_id = {}
if OUT_AGG:
    try:
        adf = pd.read_parquet(OUT_AGG)
        if "bairro_id" in adf.columns and "ngc" in adf.columns:
            adf = adf.copy()
            adf["bairro_id"] = adf["bairro_id"].apply(lambda x: extract_digits(clean_id(x)))
            ngc_by_id = dict(zip(adf["bairro_id"], adf["ngc"]))
    except Exception:
        pass

# ---------- montar FeatureCollection ----------
features = []
for _, row in g.iterrows():
    gid_num = extract_digits(row["id"])  # garante que casa com norm/rank
    props = {
        "id": gid_num,       # entregamos 'id' já normalizado (numérico em string)
        "name": row["name"],
        "level": "distrito",
        "uf": "SP",
    }

    # N.*
    norm = norm_by_id.get(gid_num, {})
    for k in N_KEYS:
        if k in norm:
            try:
                fval = float(norm[k])
                if not np.isnan(fval):
                    props[k] = float(fval)
            except Exception:
                pass

    # ranking
    if gid_num in rank_by_id:
        rinfo = rank_by_id[gid_num]
        if rinfo.get("llm_score") is not None:
            try: props["llm_score"] = float(rinfo["llm_score"])
            except: pass
        if rinfo.get("rank_sp") is not None:
            try: props["rank_sp"] = int(rinfo["rank_sp"])
            except: pass
        if rinfo.get("tier") not in (None, "", np.nan): props["tier"] = rinfo["tier"]
        if rinfo.get("drivers") not in (None, "", np.nan): props["drivers"] = rinfo["drivers"]
        if rinfo.get("explanation") not in (None, "", np.nan): props["explanation"] = rinfo["explanation"]

    # ngc (se houver)
    if gid_num in ngc_by_id and pd.notna(ngc_by_id[gid_num]):
        try: props["ngc"] = float(ngc_by_id[gid_num])
        except: pass

    features.append({
        "type":"Feature",
        "geometry": mapping(row["geometry"]) if row["geometry"] is not None else None,
        "properties": props
    })

fc = {"type":"FeatureCollection","features": features}
with open(OUT_FC,"w",encoding="utf-8") as f:
    json.dump(fc, f, ensure_ascii=False, indent=2)

print(f"[ok] FeatureCollection (norm + ranking + ngc) → {OUT_FC} | distritos: {len(features)}")
