import os, json, hashlib, tempfile, time
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, Response, Request, HTTPException
from fastapi.responses import PlainTextResponse
import brotli

# CONFIG
FINAL_FC = Path("out/distritos_front.geojson")      # arquivo que seu ETL produz
CACHE_DIR = Path("out/cdn_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "distritos_front.geojson"   # minificado (sem encoding)
CACHE_BR   = CACHE_DIR / "distritos_front.geojson.br"
ETAG_FILE  = CACHE_DIR / "distritos_front.etag"
LOCK_FILE  = CACHE_DIR / ".build.lock"

CACHE_CONTROL = "public, max-age=31536000, immutable"
CONTENT_TYPE  = "application/geo+json; charset=utf-8"

app = FastAPI()

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _atomic_write(path: Path, data: bytes, mode="wb"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=str(path.parent)) as tmp:
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)  # atomic move

def _minify_geojson_bytes(raw: bytes) -> bytes:
    # parse + dump minificado para validar JSON e remover espaços
    obj = json.loads(raw.decode("utf-8"))
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def _bro_compress(b: bytes) -> bytes:
    return brotli.compress(b, quality=11, mode=brotli.MODE_TEXT)

def _locked() -> bool:
    return LOCK_FILE.exists()

def _acquire_lock() -> bool:
    try:
        # try create exclusively
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(time.time()).encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        return False

def _release_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass

def _read_etag() -> Optional[str]:
    try:
        return ETAG_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return None

def _write_etag(etag: str):
    _atomic_write(ETAG_FILE, etag.encode("utf-8"))

def _ensure_cache(build_if_missing: bool = True) -> str:
    """
    Garante CACHE_FILE/CACHE_BR/ETAG existam e estejam sincronizados com FINAL_FC.
    Retorna etag atual.
    """
    if not FINAL_FC.exists():
        raise FileNotFoundError(f"GeoJSON fonte não encontrado: {FINAL_FC}")

    # se cache existe e é mais novo que o fonte, usa cache
    if CACHE_BR.exists() and CACHE_FILE.exists():
        if CACHE_BR.stat().st_mtime >= FINAL_FC.stat().st_mtime:
            etag = _read_etag()
            if etag:
                return etag

    if not build_if_missing and not CACHE_BR.exists():
        raise FileNotFoundError("Cache inexistente.")

    # build (pode demorar um pouco na primeira vez)
    raw = FINAL_FC.read_bytes()
    mini = _minify_geojson_bytes(raw)
    br = _bro_compress(mini)
    etag = _sha256_bytes(br)

    _atomic_write(CACHE_FILE, mini)
    _atomic_write(CACHE_BR, br)
    _write_etag(etag)
    return etag

@app.get("/geojson")
def get_geojson(request: Request):
    """
    Entrega o GeoJSON minificado + Brotli do cache.
    Se não existir, constrói na primeira chamada e já entrega.
    """
    # If-None-Match para 304
    client_etag = request.headers.get("if-none-match")

    # tenta garantir cache; se já estiver construindo por outra thread/processo, espera
    if not CACHE_BR.exists():
        # tenta lock (build inline)
        got_lock = _acquire_lock()
        try:
            if got_lock:
                etag = _ensure_cache(build_if_missing=True)
            else:
                # outro build em progresso; espera um pouco ou tenta servir parcial
                # espera até 25s (5x5s) antes de desistir
                for _ in range(5):
                    if CACHE_BR.exists():
                        break
                    time.sleep(5)
                etag = _read_etag() or _ensure_cache(build_if_missing=True)
        finally:
            if got_lock:
                _release_lock()
    else:
        # cache existe: se o fonte foi atualizado, rebuilda em linha
        if FINAL_FC.exists() and FINAL_FC.stat().st_mtime > CACHE_BR.stat().st_mtime:
            got_lock = _acquire_lock()
            try:
                if got_lock:
                    _ensure_cache(build_if_missing=True)
            finally:
                if got_lock:
                    _release_lock()
        etag = _read_etag() or _ensure_cache(build_if_missing=True)

    # 304
    if client_etag and etag and client_etag.strip('"') == etag:
        return Response(status_code=304)

    # serve .br
    data = CACHE_BR.read_bytes()
    return Response(
        content=data,
        media_type=CONTENT_TYPE,
        headers={
            "Content-Encoding": "br",
            "Cache-Control": CACHE_CONTROL,
            "ETag": f"\"{etag}\""
        }
    )

@app.post("/geojson/rebuild")
def rebuild():
    """
    Força rebuild do cache. Se já houver build em progresso, retorna 202 + Retry-After.
    """
    if _locked():
        # já tem build rolando
        return Response(
            status_code=202,
            headers={"Retry-After": "5"},
            content="build em andamento, tente novamente em alguns segundos"
        )

    got_lock = _acquire_lock()
    if not got_lock:
        return Response(
            status_code=202,
            headers={"Retry-After": "5"},
            content="build em andamento, tente novamente em alguns segundos"
        )

    try:
        etag = _ensure_cache(build_if_missing=True)
    finally:
        _release_lock()

    return PlainTextResponse(f"ok - etag: {etag}", status_code=200)
