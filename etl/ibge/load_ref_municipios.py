# etl/ibge/load_ref_municipios.py
import os, re, sys, unicodedata
from pathlib import Path
from itertools import islice
import pandas as pd
from pymongo import MongoClient, UpdateOne
from etl.common.config import load_settings

# ---------- Helpers ----------
def _strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return unicodedata.normalize("NFC", s)

def _norm_col(s: str) -> str:
    s = (s or "").strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _to_int_safe(x):
    if pd.isna(x): return None
    s = re.sub(r"\D", "", str(x))
    return int(s) if s else None

def _to_pop_int(x):
    if pd.isna(x): return 0
    s = str(x).strip().replace(".", "").replace(",", ".")
    try: return int(float(s))
    except Exception: return 0

def _read_csv_any(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc, dtype=str)
        except Exception:
            pass
    return pd.read_csv(path, dtype=str)

def _batches(it, size=1000):
    it = iter(it)
    while True:
        chunk = list(islice(it, size))
        if not chunk: break
        yield chunk

# ---------- Candidatos de nomes ----------
CAND_MUN_ID   = {"municipio_ibge", "codigo_ibge", "cod_municipio", "codigo", "codigo_municipio"}
CAND_MUN_NAME = {"municipio", "nome_municipio", "municipio_nome", "nome"}
CAND_UF       = {"uf", "uf_sigla", "sigla_uf", "estado_sigla"}
CAND_UF_CODE  = {"cod_uf", "codigo_uf", "uf_codigo"}
CAND_POP      = {"populacao", "populacao_estimada", "populacao_total", "habitantes", "pop_total"}
CAND_LAT      = {"lat", "latitude"}
CAND_LON      = {"lon", "long", "longitude"}
CAND_AREA     = {"area_km2", "area", "area_total_km2"}

def _pick_col(df: pd.DataFrame, cands: set[str]):
    for c in df.columns:
        if _norm_col(c) in cands:
            return c
    return None

# ---------- Pipeline ----------
def load_csv_to_dataframe(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    raw = _read_csv_any(csv_path)
    if raw.empty:
        raise ValueError("CSV lido está vazio.")

    # Normaliza nomes
    rename_map = {c: _norm_col(c) for c in raw.columns}
    df = raw.rename(columns=rename_map).copy()

    # Detecta colunas
    col_mun_id   = _pick_col(df, CAND_MUN_ID)
    col_mun_name = _pick_col(df, CAND_MUN_NAME)
    col_uf       = _pick_col(df, CAND_UF)
    col_uf_code  = _pick_col(df, CAND_UF_CODE)
    col_pop      = _pick_col(df, CAND_POP)
    col_lat      = _pick_col(df, CAND_LAT)
    col_lon      = _pick_col(df, CAND_LON)
    col_area     = _pick_col(df, CAND_AREA)

    if not col_mun_id:
        raise ValueError("Não foi possível identificar a coluna do código do município (codigo_ibge).")

    # Chave IBGE (7 dígitos): usa já-pronto ou compõe COD_UF(2) + MUN(5)
    mun_id_series = df[col_mun_id].apply(_to_int_safe)
    if col_uf_code is not None:
        lengths = mun_id_series.dropna().astype(int).astype(str).str.len()
        if not lengths.empty and (lengths.median() <= 5 or lengths.max() <= 6):
            uf_code = df[col_uf_code].apply(_to_int_safe).fillna(0).astype(int)
            mun5 = mun_id_series.fillna(0).astype(int)
            mun_id_series = (uf_code * 100000 + mun5).astype(int)

    if not col_mun_name:
        raise ValueError("Coluna com nome do município não encontrada.")
    municipio_series = df[col_mun_name].astype(str).str.strip()

    if col_uf:
        uf_series = df[col_uf].astype(str).str.upper().str.strip()
    else:
        uf_series = pd.Series([None] * len(df))

    pop_series = df[col_pop].apply(_to_pop_int).astype(int) if col_pop else pd.Series([0] * len(df), dtype=int)
    lat_series  = pd.to_numeric(df[col_lat], errors="coerce") if col_lat else pd.Series([None]*len(df))
    lon_series  = pd.to_numeric(df[col_lon], errors="coerce") if col_lon else pd.Series([None]*len(df))
    area_series = pd.to_numeric(df[col_area], errors="coerce") if col_area else pd.Series([None]*len(df))

    out = pd.DataFrame({
        "codigo_ibge": mun_id_series.astype("Int64"),
        "municipio": municipio_series,
        "uf_sigla": uf_series,
        "populacao": pop_series,
        "lat": lat_series,
        "lon": lon_series,
        "area_km2": area_series
    })

    out = out.dropna(subset=["codigo_ibge"])
    out["codigo_ibge"] = out["codigo_ibge"].astype(int)
    out = out.drop_duplicates(subset=["codigo_ibge"], keep="last")
    return out

def mongo_upsert(df: pd.DataFrame, mongo_uri: str, batch_size: int = 1000) -> int:
    cli = MongoClient(mongo_uri)
    db = cli.get_database()
    col = db.get_collection("ref_municipios")

    records = df.to_dict(orient="records")
    total = 0
    for batch in _batches(records, batch_size):
        ops = [
            UpdateOne(
                {"codigo_ibge": int(r["codigo_ibge"])},
                {"$set": r},
                upsert=True
            )
            for r in batch
        ]
        res = col.bulk_write(ops, ordered=False)
        total += (res.upserted_count or 0) + (res.modified_count or 0)
    cli.close()
    return total

def main(csv_path="data/ref/municipios.csv"):
    s = load_settings()
    df = load_csv_to_dataframe(csv_path)
    n = mongo_upsert(df, s.mongo_uri, 1000)
    print(f"[ref_municipios] Registros inseridos/atualizados: {n}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Carrega municipios para ref_municipios (Mongo).")
    ap.add_argument("--csv", default="data/ref/municipios.csv", help="Caminho do CSV de municipios (pode ser 'bruto').")
    args = ap.parse_args()
    try:
        main(args.csv)
    except Exception as e:
        print("ERRO:", e, file=sys.stderr)
        sys.exit(1)
