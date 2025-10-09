# etl/weather/fetch_weather.py
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError

from etl.common.config import load_settings
from etl.common.dateutils import utc_now, last_n_days_window
from etl.common.httpclient import get_client

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Conjunto padrão de variáveis horárias (pode sobrescrever via .env OPENMETEO_HOURLY)
DEFAULT_HOURLY = (
    "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,"
    "wind_gusts_10m,cloud_cover,dew_point_2m"
).split(",")


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_target_cities(mongo_uri: str, days: int = 7) -> pd.DataFrame:
    """
    Retorna um DataFrame com os municípios que tiveram focos nos últimos N dias,
    agregando lat/lon médios por municipio_ibge (fallback por (municipio, uf)).
    """
    start, end = last_n_days_window(days)
    cli = MongoClient(mongo_uri)
    db = cli.get_database()

    pipe = [
        {"$match": {"ts": {"$gte": start, "$lte": end}}},
        {"$group": {
            "_id": {
                "mun_id": "$meta.municipio_ibge",
                "municipio": "$meta.municipio",
                "uf": "$meta.uf"
            },
            "lat": {"$avg": "$lat"},
            "lon": {"$avg": "$lon"},
            "focos": {"$sum": 1}
        }},
        {"$sort": {"focos": -1}},
        {"$limit": 5000}
    ]
    rows = list(db.raw_fires.aggregate(pipe))
    cli.close()

    if not rows:
        return pd.DataFrame(columns=["municipio_ibge", "municipio", "uf", "lat", "lon", "focos"])

    recs = []
    for r in rows:
        _id = r["_id"] or {}
        recs.append({
            "municipio_ibge": _id.get("mun_id"),
            "municipio": _id.get("municipio"),
            "uf": _id.get("uf"),
            "lat": r.get("lat"),
            "lon": r.get("lon"),
            "focos": r.get("focos", 0),
        })
    df = pd.DataFrame(recs)
    # sanidade
    df = df.dropna(subset=["lat", "lon"])
    df = df[(df["lat"].between(-90, 90)) & (df["lon"].between(-180, 180))]
    return df.sort_values("focos", ascending=False)


def fetch_city_hourly(http, db, city_row, start: datetime, end: datetime, hourly_vars: list[str]) -> int:
    """
    Busca dados horários na Open-Meteo para uma cidade e grava em raw_weather (time-series)
    com deduplicação via coleção normal dedup_weather_mun_ts (unique (municipio_ibge, ts)).
    Retorna o número de documentos inseridos.
    """
    lat = float(city_row["lat"])
    lon = float(city_row["lon"])
    mun_id = city_row.get("municipio_ibge")
    mun_id = int(mun_id) if pd.notna(mun_id) else None
    uf = city_row.get("uf")

    params = {
        "latitude": f"{lat:.5f}",
        "longitude": f"{lon:.5f}",
        "hourly": ",".join(hourly_vars),
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "timezone": "UTC",
    }

    r = http.get(OPEN_METEO_URL, params=params)
    r.raise_for_status()
    data = r.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return 0

    keys = [k for k in hourly.keys() if k != "time"]

    # coleções
    col_ts = db.get_collection("raw_weather")               # time-series
    col_dedup = db.get_collection("dedup_weather_mun_ts")   # normal com unique (municipio_ibge, ts)

    inserted = 0
    for i, t in enumerate(times):
        ts_utc = datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
        doc = {
            "ts": ts_utc,
            "meta": {"municipio_ibge": mun_id, "uf": uf},
            "lat": lat,
            "lon": lon,
            "source": "open-meteo",
        }
        for k in keys:
            vlist = hourly.get(k, [])
            if i < len(vlist):
                doc[k] = vlist[i]

        if mun_id is None:
            # Sem municipio_ibge: não é possível deduplicar por chave única — insere direto.
            col_ts.insert_one(doc)
            inserted += 1
            continue

        try:
            # Reserva chave na coleção de dedupe; se já existir, ignora
            col_dedup.insert_one({"municipio_ibge": mun_id, "ts": ts_utc})
        except DuplicateKeyError:
            continue

        # Grava no time-series
        col_ts.insert_one(doc)
        inserted += 1

    return inserted


def main():
    s = load_settings()
    days = int(os.environ.get("WEATHER_LOOKBACK_DAYS", "7"))
    hourly_vars = os.environ.get("OPENMETEO_HOURLY", ",".join(DEFAULT_HOURLY)).split(",")
    timeout = int(os.environ.get("HTTP_TIMEOUT", "30"))
    max_workers = int(os.environ.get("MAX_WORKERS", "6"))

    # Janela UTC (horária)
    start, end = last_n_days_window(days)
    start = start.replace(minute=0, second=0, microsecond=0)
    end = _to_utc(utc_now()).replace(minute=0, second=0, microsecond=0)

    # Municípios alvo com base em raw_fires
    cities_df = get_target_cities(s.mongo_uri, days=days)
    if cities_df.empty:
        print("[weather] Nenhum município alvo nos últimos", days, "dias.")
        return

    http_client = get_client(timeout=timeout)
    mongo = MongoClient(s.mongo_uri)
    db = mongo.get_database()

    total = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(fetch_city_hourly, http_client, db, row, start, end, hourly_vars)
            for _, row in cities_df.iterrows()
        ]
        for fut in as_completed(futures):
            try:
                total += fut.result()
            except Exception as e:
                print("[WARN]", e)

    mongo.close()
    http_client.close()
    print(f"[weather] Inseridos (após dedupe): {total}")


if __name__ == "__main__":
    main()
