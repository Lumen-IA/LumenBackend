# fix_geojson_names.py
import geopandas as gpd

SRC = "data/raw/sao_paulo_distritos.geojson"   # coloque o arquivo que tem NOME_DIST/CD_DIST
DST = "data/raw/sao_paulo_distritos.geojson"           # este é o que o ETL usa

g = gpd.read_file(SRC).to_crs(4326)

# Renomeia as colunas oficiais (ajuste se seu arquivo tiver nomes ligeiramente diferentes)
cols = {c.lower(): c for c in g.columns}
if "nome_dist" in cols:
    g = g.rename(columns={cols["nome_dist"]: "name"})
elif "distritos" in cols:
    g = g.rename(columns={cols["distritos"]: "name"})
elif "nome" in cols:
    g = g.rename(columns={cols["nome"]: "name"})
else:
    raise RuntimeError("Não encontrei coluna de nome ('NOME_DIST', 'Distritos' ou similar) no GeoJSON oficial.")

if "cd_dist" in cols:
    g = g.rename(columns={cols["cd_dist"]: "id"})
elif "codigo" in cols:
    g = g.rename(columns={cols["codigo"]: "id"})
elif "id" not in g.columns:
    g["id"] = [f"distrito_{i}" for i in range(len(g))]

g = g[["id","name","geometry"]].copy()
g.to_file(DST, driver="GeoJSON")
print(f"[ok] corrigido: {DST} com 'id' e 'name' oficiais.")
