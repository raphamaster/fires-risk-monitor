# etl/gold/export_parquet.py
import os
from pathlib import Path

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient

from etl.common.config import load_settings
from etl.common.dateutils import last_n_days_window

PARQUET_ROOT = "data/gold"

def _write_partitioned(df: pd.DataFrame, base: Path, part_cols: list[str]):
    base.mkdir(parents=True, exist_ok=True)
    # evita valores incompatíveis com parquet
    df = df.replace([np.inf, -np.inf], np.nan)
    for keys, sub in df.groupby(part_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        sub = sub.drop(columns=[c for c in part_cols if c in sub.columns], errors="ignore")
        parts = []
        for col, val in zip(part_cols, keys):
            sval = "" if pd.isna(val) else str(val)
            parts.append(f"{col}={sval}")
        out_dir = base.joinpath(*parts)
        out_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(sub, preserve_index=False), out_dir / "part.parquet")

def _write_single(df: pd.DataFrame, out_file: Path):
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df = df.replace([np.inf, -np.inf], np.nan)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_file)

def build_fact_fires_daily(mongo_uri: str, lookback_days: int = 180) -> pd.DataFrame:
    start, end = last_n_days_window(lookback_days)
    cli = MongoClient(mongo_uri); db = cli.get_database()
    pipe = [
        {"$match": {"ts": {"$gte": start, "$lte": end}}},
        {"$match": {"meta.municipio_ibge": {"$ne": None}}},
        {"$project": {
            "date": {"$dateTrunc": {"date": "$ts", "unit": "day"}},
            "municipio_ibge": "$meta.municipio_ibge",
            "uf": "$meta.uf",
            "confianca": "$confianca"
        }},
        {"$group": {
            "_id": {"date":"$date","mun":"$municipio_ibge","uf":"$uf"},
            "focos": {"$sum": 1},
            "p95_conf": {
                "$percentile": {
                    "input": {"$ifNull": ["$confianca", None]},
                    "p": [0.95],
                    "method": "approximate"
                }
            }
        }},
        {"$project": {
            "_id":0,
            "date":"$_id.date",
            "municipio_ibge":"$_id.mun",
            "uf":"$_id.uf",
            "focos":1,
            "p95_conf":{"$arrayElemAt":["$p95_conf",0]}
        }}
    ]
    rows = list(db.raw_fires.aggregate(pipe)); cli.close()
    if not rows:
        return pd.DataFrame(columns=["date","municipio_ibge","uf","focos","p95_conf","year","month"])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.date
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month.astype(int)
    # ordena colunas
    df = df[["date","municipio_ibge","uf","focos","p95_conf","year","month"]]
    return df

def build_weather_daily(mongo_uri: str, lookback_days: int = 180) -> pd.DataFrame:
    start, end = last_n_days_window(lookback_days)
    cli = MongoClient(mongo_uri); db = cli.get_database()
    pipe = [
        {"$match": {"ts": {"$gte": start, "$lte": end}}},
        {"$match": {"meta.municipio_ibge": {"$ne": None}}},
        {"$project": {
            "date": {"$dateTrunc": {"date": "$ts", "unit": "day"}},
            "municipio_ibge": "$meta.municipio_ibge",
            "uf": "$meta.uf",
            "temperature_2m": 1,
            "relative_humidity_2m": 1,
            "wind_speed_10m": 1,
            "wind_gusts_10m": 1,
            "cloud_cover": 1,
            "precipitation": 1,
            "dew_point_2m": 1
        }},
        {"$group": {
            "_id": {"date":"$date","mun":"$municipio_ibge","uf":"$uf"},
            "temp_mean":{"$avg":"$temperature_2m"},
            "hum_min":{"$min":"$relative_humidity_2m"},
            "wind_max":{"$max":"$wind_speed_10m"},
            "gust_max":{"$max":"$wind_gusts_10m"},
            "cloud_mean":{"$avg":"$cloud_cover"},
            "precip_sum":{"$sum":"$precipitation"},
            "dew_mean":{"$avg":"$dew_point_2m"}
        }},
        {"$project":{
            "_id":0,"date":"$_id.date","municipio_ibge":"$_id.mun","uf":"$_id.uf",
            "temp_mean":1,"hum_min":1,"wind_max":1,"gust_max":1,"cloud_mean":1,"precip_sum":1,"dew_mean":1
        }}
    ]
    rows = list(db.raw_weather.aggregate(pipe)); cli.close()
    if not rows:
        return pd.DataFrame(columns=[
            "date","municipio_ibge","uf","temp_mean","hum_min","wind_max","gust_max","cloud_mean","precip_sum","dew_mean",
            "year","month"
        ])
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.date
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month.astype(int)
    df = df[[
        "date","municipio_ibge","uf","temp_mean","hum_min","wind_max","gust_max","cloud_mean","precip_sum","dew_mean",
        "year","month"
    ]]
    return df

def load_dim_municipio(mongo_uri: str) -> pd.DataFrame:
    cli = MongoClient(mongo_uri); db = cli.get_database()
    rows = list(db.ref_municipios.find({}, {"_id":0}))
    cli.close()
    if not rows:
        return pd.DataFrame(columns=["municipio_ibge","municipio","uf","populacao","lat","lon","area_km2"])
    df = pd.DataFrame(rows)
    # padroniza nomes/ordem
    if "municipio_ibge" not in df.columns and "codigo_ibge" in df.columns:
        df = df.rename(columns={"codigo_ibge": "municipio_ibge"})
    if "uf" not in df.columns and "uf_sigla" in df.columns:
        df = df.rename(columns={"uf_sigla": "uf"})
    keep = ["municipio_ibge","municipio","uf","populacao","lat","lon","area_km2"]
    for k in keep:
        if k not in df.columns:
            df[k] = np.nan if k not in ("municipio_ibge","municipio","uf","populacao") else (0 if k=="populacao" else None)
    df = df[keep]
    return df

def _safe_norm(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    mn, mx = s.min(skipna=True), s.max(skipna=True)
    if pd.isna(mn) or pd.isna(mx) or mx == mn:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - mn) / (mx - mn)

def build_fact_risk_daily(fires_df: pd.DataFrame, weather_df: pd.DataFrame, dim_mun: pd.DataFrame) -> pd.DataFrame:
    f = fires_df.copy()
    w = weather_df.copy()
    d = dim_mun.copy()

    for df in (f, w, d):
        if "municipio_ibge" in df.columns:
            df["municipio_ibge"] = pd.to_numeric(df["municipio_ibge"], errors="coerce").astype("Int64")

    if w.empty:
        return pd.DataFrame(columns=[
            "date","municipio_ibge","uf","temp_mean","hum_min","wind_max","gust_max","cloud_mean","precip_sum",
            "focos","focos_3d","focos_3d_100k","risk_score","year","month"
        ])

    f3 = f.sort_values(["municipio_ibge","date"]).copy()
    if not f3.empty:
        f3["focos_3d"] = f3.groupby("municipio_ibge", dropna=False)["focos"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    else:
        f3["focos_3d"] = pd.Series(dtype=float)

    fw = pd.merge(
        w,
        f3[["municipio_ibge","date","focos","focos_3d","uf"]] if not f3.empty else
        w.assign(focos=np.nan, focos_3d=np.nan)[["municipio_ibge","date","focos","focos_3d","uf"]],
        on=["municipio_ibge","date","uf"],
        how="left"
    )

    if not d.empty and "populacao" in d.columns:
        fwd = pd.merge(fw, d[["municipio_ibge","populacao"]], on="municipio_ibge", how="left")
    else:
        fwd = fw.copy()
        fwd["populacao"] = np.nan

    fwd["populacao"] = pd.to_numeric(fwd["populacao"], errors="coerce")
    fwd["focos_3d_100k"] = np.where(
        (fwd["populacao"].notna()) & (fwd["populacao"] > 0),
        fwd["focos_3d"] / (fwd["populacao"] / 100000.0),
        np.nan
    )

    comp_wind  = _safe_norm(fwd["wind_max"])
    comp_hum   = _safe_norm((100 - pd.to_numeric(fwd["hum_min"], errors="coerce").clip(0,100)))
    comp_rain  = _safe_norm((10 - pd.to_numeric(fwd["precip_sum"], errors="coerce")).clip(lower=0))
    comp_fires = _safe_norm(fwd["focos_3d_100k"].fillna(0))

    fwd["risk_score"] = (comp_wind + comp_hum + comp_rain + comp_fires) / 4.0

    out = fwd[[
        "date","municipio_ibge","uf","temp_mean","hum_min","wind_max","gust_max","cloud_mean","precip_sum",
        "focos","focos_3d","focos_3d_100k","risk_score","year","month"
    ]].copy()
    return out

def main():
    s = load_settings()

    fires_df   = build_fact_fires_daily(s.mongo_uri, lookback_days=180)
    weather_df = build_weather_daily(s.mongo_uri, lookback_days=180)
    dim_mun    = load_dim_municipio(s.mongo_uri)

    root = Path(PARQUET_ROOT)

    # fact_fires_daily
    if not fires_df.empty:
        _write_partitioned(fires_df, root / "fact_fires_daily", ["uf","year","month"])
        _write_single(
            fires_df.sort_values(["uf","date","municipio_ibge"]),
            root / "fact_fires_daily.parquet"
        )
        print("[gold] fact_fires_daily OK (partitioned + single)")
    else:
        print("[gold] fact_fires_daily: vazio")

    # fact_risk_daily
    if not weather_df.empty:
        risk_df = build_fact_risk_daily(fires_df, weather_df, dim_mun)
        _write_partitioned(risk_df, root / "fact_risk_daily", ["uf","year","month"])
        _write_single(
            risk_df.sort_values(["uf","date","municipio_ibge"]),
            root / "fact_risk_daily.parquet"
        )
        print("[gold] fact_risk_daily OK (partitioned + single)")
    else:
        print("[gold] fact_risk_daily: clima vazio")

    # dim_municipio
    if not dim_mun.empty:
        # arquivo único (e ainda mantemos o antigo dentro da pasta)
        _write_single(dim_mun, root / "dim_municipio.parquet")
        (root / "dim_municipio").mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(dim_mun, preserve_index=False), root / "dim_municipio" / "dim_municipio.parquet")
        print("[gold] dim_municipio OK (single + folder)")
    else:
        print("[gold] dim_municipio: vazio")

if __name__ == "__main__":
    main()
