import json, yaml, requests, numpy as np, pandas as pd, time

cfg = yaml.safe_load(open("config.yaml","r",encoding="utf-8"))
LLM = cfg["llm"]
IN_NORM = cfg["outputs"]["norm_json"]
OUT_RANK = cfg["outputs"]["rank_json"]

# Carrega o pacote para o LLM
payload_in = json.load(open(IN_NORM,"r",encoding="utf-8"))
items = payload_in.get("distritos", [])
N = len(items)

if N == 0:
    raise SystemExit("[erro] norm_for_llm.json não tem distritos.")

# Features permitidas (devem existir no 'norm' de cada item)
# ideb_good é opcional; vamos detectar dinamicamente
base_features = ["marginalidade","schools_total_bad","share_municipal_bad","share_estadual_bad","acesso_creche_bad"]
has_ideb = any("ideb_good" in it.get("norm", {}) for it in items)
allowed_features = base_features + (["ideb_good"] if has_ideb else [])

# Schema rígido para saída do LLM
LLM_SCHEMA = {
  "type":"object",
  "required":["ranking"],
  "properties":{
    "ranking":{
      "type":"array",
      "minItems": N,
      "maxItems": N,
      "items":{
        "type":"object",
        "required":["id","rank","llm_score","tier","drivers","explanation"],
        "properties":{
          "id":{"type":"string"},
          "rank":{"type":"integer","minimum":1},
          "llm_score":{"type":"number","minimum":0,"maximum":1},
          "tier":{"type":"string","enum":["muito alto","alto","médio","baixo","muito baixo"]},
          "drivers":{
            "type":"array",
            "minItems": 3,
            "maxItems": 3,
            "items":{
              "type":"object",
              "required":["name","direction","contribution"],
              "properties":{
                "name":{"type":"string","enum": allowed_features},
                "direction":{"type":"string","enum":["up","down"]},
                "contribution":{"type":"number"}
              }
            }
          },
          "explanation":{"type":"string"}
        }
      }
    }
  }
}

# Prompt extremamente explícito
SYSTEM = (
  "Você é um avaliador técnico. Recebe uma lista de distritos com indicadores normalizados em 'norm'. "
  "Há exatamente {N} distritos de entrada, e você deve devolver um array 'ranking' COM EXATAMENTE {N} ITENS "
  "(um por distrito), NA MESMA ORDEM DOS 'id' de entrada. "
  "Regras dos indicadores (valem para TODOS os distritos):\n"
  "- 'marginalidade': 1=pior (mais vulnerável)\n"
  "- 'schools_total_bad': 1=pior (pouca oferta relativa)\n"
  "- 'share_municipal_bad': 1=pior (menor participação municipal)\n"
  "- 'share_estadual_bad': 1=pior (menor participação estadual)\n"
  "- 'acesso_creche_bad': 1=pior (baixo acesso)\n"
  + ("- 'ideb_good': 1=melhor (melhor IDEB)\n" if has_ideb else "") +
  "Calcule um 'llm_score' (0..1, maior=pior) para cada distrito com base nessas features. "
  "Ordene do pior para o melhor ('rank': 1 = pior situação). "
  "Defina 'tier' por quantis do llm_score (>=0.8 muito alto; >=0.6 alto; >=0.4 médio; >=0.2 baixo; senão muito baixo). "
  "Em 'drivers' liste EXATAMENTE 3 itens com 'name' sendo APENAS um dos nomes de features PERMITIDAS, "
  "'direction' = 'up' para piora (aumenta score) e 'down' para melhora (reduz score), e 'contribution' ≈ impacto relativo (soma ~<=1). "
  "NÃO use nomes de distritos como drivers. Não escreva texto extra. Retorne APENAS JSON no schema."
).format(N=N)

# constrói um mapa id->norm para ancorar drivers
norm_map = {str(d["id"]): d.get("norm", {}) for d in items}
# normaliza chaves (caso id venha com .0)
norm_map = {str(k).replace(".0",""): v for k,v in norm_map.items()}


def call_llm(distritos, system_prompt, schema, temperature=0):
    url = LLM["url"]
    model = LLM["model"]
    req = {
        "model": model,
        "messages": [
            {"role":"system","content": system_prompt},
            {"role":"user","content": json.dumps({"distritos": distritos}, ensure_ascii=False)}
        ],
        "stream": False,
        "format": schema,
        "options": {"temperature": temperature}
    }
    r = requests.post(url, json=req, timeout=300)
    r.raise_for_status()
    return json.loads(r.json()["message"]["content"])

def sanitize_and_complete(out, distritos, allowed_feats):
    df = pd.DataFrame(out.get("ranking", []))
    if df.empty or len(df) < len(distritos):
        return None

    input_ids = [str(d["id"]).replace(".0","") for d in distritos]
    df["id"] = df["id"].astype(str).str.replace(r"\.0$","", regex=True)
    df = df.set_index("id", drop=False)

    # completa ausentes
    for i in input_ids:
        if i not in df.index:
            df.loc[i] = {"id": i, "rank": None, "llm_score": None, "tier": None,
                         "drivers": [], "explanation": ""}

    df = df.loc[input_ids].reset_index(drop=True)

    # clamp score
    df["llm_score"] = pd.to_numeric(df["llm_score"], errors="coerce").fillna(0.0).clip(0,1)

    # >>> regravar drivers com os valores reais do 'norm'
    def fix_drivers(row):
        did = row["id"]
        nv  = norm_map.get(did, {})
        raw = row["drivers"] if isinstance(row["drivers"], list) else []
        out = []
        for it in raw:
            nm = it.get("name")
            if nm in allowed_feats:
                val = nv.get(nm, 0.0)
                try:
                    val = float(val)
                except:
                    val = 0.0
                out.append({"name": nm, "direction": "up" if val>=0 else "down", "contribution": float(val)})
        # completar se vier menos de 3
        while len(out) < 3:
            nm = [f for f in allowed_feats if f in nv]
            nm = (nm + allowed_feats)[len(out) % len(allowed_feats)]
            val = float(nv.get(nm, 0.0))
            out.append({"name": nm, "direction":"up" if val>=0 else "down", "contribution": val})
        return out[:3]

    df["drivers"] = df.apply(fix_drivers, axis=1)

    # reordena por score e recalcula rank/tier localmente
    df = df.sort_values(["llm_score","id"], ascending=[False, True]).reset_index(drop=True)
    df["rank_sp"] = np.arange(1, len(df)+1)

    q = df["llm_score"].rank(pct=True)
    def tier_of(p):
        if p >= 0.8: return "muito alto"
        if p >= 0.6: return "alto"
        if p >= 0.4: return "médio"
        if p >= 0.2: return "baixo"
        return "muito baixo"
    df["tier"] = q.apply(tier_of)

    return {"ranking": df[["id","llm_score","rank_sp","tier","drivers","explanation"]].rename(columns={"rank_sp":"rank"}).to_dict(orient="records")}

# 1ª tentativa com schema rígido
try:
    out = call_llm(items, SYSTEM, LLM_SCHEMA, temperature=LLM.get("temperature", 0))
    result = sanitize_and_complete(out, items, allowed_features)
except Exception as e:
    result = None

# 2ª tentativa (se necessário), com uma instrução ainda mais explícita
if result is None:
    time.sleep(0.5)
    SYSTEM_RETRY = SYSTEM + "\nATENÇÃO: O array 'ranking' deve ter EXATAMENTE {N} itens, UM para CADA 'id' na MESMA ORDEM recebida.".format(N=N)
    try:
        out = call_llm(items, SYSTEM_RETRY, LLM_SCHEMA, temperature=LLM.get("temperature", 0))
        result = sanitize_and_complete(out, items, allowed_features)
    except Exception:
        result = None

# Fallback determinístico se o LLM não cumprir o contrato
if result is None:
    df_in = pd.DataFrame([{"id": d["id"], **d["norm"]} for d in items])
    # score simples: média das features (ideb_good entra negativo porque 1=melhor)
    feats = base_features + (["ideb_good"] if has_ideb else [])
    score = df_in[base_features].mean(axis=1)
    if has_ideb:
        score = (score*len(base_features) + (1 - df_in["ideb_good"])) / (len(base_features)+1)  # ideb_good reduz score
    df_in["llm_score"] = score.clip(0,1)
    df_in = df_in.sort_values(["llm_score","id"], ascending=[False, True]).reset_index(drop=True)
    df_in["rank"] = np.arange(1, len(df_in)+1)

    q = df_in["llm_score"].rank(pct=True)
    def tier_of(p):
        if p >= 0.8: return "muito alto"
        if p >= 0.6: return "alto"
        if p >= 0.4: return "médio"
        if p >= 0.2: return "baixo"
        return "muito baixo"
    df_in["tier"] = q.apply(tier_of)

    # drivers heurísticos: top-3 variáveis mais desfavoráveis (valores maiores após sinal adequado)
    drv_df = df_in[feats].copy()
    # para ideb_good (1=melhor), transformar para 'bad' temporário para escolher drivers
    if has_ideb:
        drv_df["ideb_good"] = 1 - drv_df["ideb_good"]
    out_rows = []
    for i, r in df_in.iterrows():
        vv = drv_df.loc[i, feats].sort_values(ascending=False).head(3)
        drivers = [{"name": k, "direction":"up", "contribution": float(v)} for k, v in vv.items()]
        out_rows.append({
            "id": str(r["id"]),
            "rank": int(r["rank"]),
            "llm_score": float(round(r["llm_score"], 6)),
            "tier": r["tier"],
            "drivers": drivers,
            "explanation": ""
        })
    result = {"ranking": out_rows}

# grava saída
json.dump(result, open(OUT_RANK,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
print(f"[ok] ranking LLM: {OUT_RANK} (itens: {len(result['ranking'])}/{N})")
