# etl/ibge/load_coords_csv.py
import sys, re
import pandas as pd
from pymongo import MongoClient, UpdateOne
from etl.common.config import load_settings

CAND_ID  = {"codigo_ibge","municipio_ibge","cod_municipio","ibge","codigo","codigo_municipio"}
CAND_LAT = {"lat","latitude"}
CAND_LON = {"lon","long","longitude"}

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("á","a").replace("à","a").replace("ã","a").replace("â","a") \
         .replace("é","e").replace("ê","e").replace("í","i").replace("ó","o") \
         .replace("ô","o").replace("õ","o").replace("ú","u").replace("ç","c")
    s = re.sub(r"[^a-z0-9]+","_", s)
    s = re.sub(r"_+","_", s).strip("_")
    return s

def _pick(df: pd.DataFrame, candidates: set[str]) -> str | None:
    for c in df.columns:
        if _norm(c) in candidates:
            return c
    return None

def _read_csv_any(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","latin1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc, dtype=str)
        except Exception:
            pass
    return pd.read_csv(path, dtype=str)

def load_coords(csv_path: str) -> pd.DataFrame:
    df = _read_csv_any(csv_path)
    if df.empty:
        raise ValueError("CSV vazio.")

    # renomeia colunas para norm
    df = df.rename(columns={c:_norm(c) for c in df.columns})

    id_col  = _pick(df, CAND_ID)
    lat_col = _pick(df, CAND_LAT)
    lon_col = _pick(df, CAND_LON)
    missing = [n for n,v in {"id":id_col,"lat":lat_col,"lon":lon_col}.items() if v is None]
    if missing:
        raise ValueError(f"Colunas não encontradas no CSV: {missing} (aceitas id={CAND_ID}, lat={CAND_LAT}, lon={CAND_LON})")

    # converte tipos
    def to_int(x):
        if pd.isna(x): return None
        s = re.sub(r"\D","", str(x))
        return int(s) if s else None

    out = pd.DataFrame({
        "codigo_ibge": df[id_col].map(to_int),
        "lat": pd.to_numeric(df[lat_col], errors="coerce"),
        "lon": pd.to_numeric(df[lon_col], errors="coerce"),
    })

    # saneamento básico
    out = out.dropna(subset=["codigo_ibge","lat","lon"])
    out["codigo_ibge"] = out["codigo_ibge"].astype(int)
    out = out[(out["lat"].between(-90,90)) & (out["lon"].between(-180,180))]
    out = out.drop_duplicates(subset=["codigo_ibge"], keep="last")
    return out

def upsert_coords(df: pd.DataFrame, overwrite: bool) -> int:
    s = load_settings()
    cli = MongoClient(s.mongo_uri); db = cli.get_database()
    col = db.ref_municipios

    ops = []
    for r in df.to_dict(orient="records"):
        filt = {"codigo_ibge": int(r["codigo_ibge"])}
        if overwrite:
            upd = {"$set": {"lat": float(r["lat"]), "lon": float(r["lon"])}}
        else:
            # só atualiza se lat/lon estiverem ausentes ou nulos
            upd = {"$set": {"lat": float(r["lat"]), "lon": float(r["lon"])}}
        ops.append(UpdateOne(filt, upd, upsert=False))  # não cria novos municípios

    if not ops:
        cli.close()
        return 0
    res = col.bulk_write(ops, ordered=False)
    cli.close()
    return res.modified_count

def main(csv_path: str, overwrite: bool):
    df = load_coords(csv_path)
    n = upsert_coords(df, overwrite=overwrite)
    print(f"[coords-csv] Municípios atualizados (lat/lon): {n}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Atualiza ref_municipios com lat/lon a partir de CSV.")
    ap.add_argument("--csv", required=True, help="Caminho do CSV com codigo_ibge, lat, lon (nomes flexíveis).")
    ap.add_argument("--overwrite", action="store_true", help="Se presente, sobrescreve lat/lon existentes.")
    args = ap.parse_args()
    try:
        main(args.csv, overwrite=args.overwrite)
    except Exception as e:
        print("ERRO:", e, file=sys.stderr)
        sys.exit(1)
