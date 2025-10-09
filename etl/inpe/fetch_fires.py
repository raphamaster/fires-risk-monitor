import io, sys, csv, httpx
from tqdm import tqdm
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone
from dateutil import parser as dtparser
from etl.common.config import load_settings
from etl.common.dateutils import last_n_days_window

# Mapa UF nome->sigla e id->sigla (IBGE)
UF_NOME2SIGLA = {
    "ACRE":"AC","ALAGOAS":"AL","AMAPÁ":"AP","AMAPA":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARÁ":"CE","CEARA":"CE",
    "DISTRITO FEDERAL":"DF","ESPÍRITO SANTO":"ES","ESPIRITO SANTO":"ES","GOIÁS":"GO","GOIAS":"GO","MARANHÃO":"MA","MARANHAO":"MA",
    "MATO GROSSO":"MT","MATO GROSSO DO SUL":"MS","MINAS GERAIS":"MG","PARÁ":"PA","PARA":"PA","PARAÍBA":"PB","PARAIBA":"PB",
    "PARANÁ":"PR","PARANA":"PR","PERNAMBUCO":"PE","PIAUÍ":"PI","PIAUI":"PI","RIO DE JANEIRO":"RJ","RIO GRANDE DO NORTE":"RN",
    "RIO GRANDE DO SUL":"RS","RONDÔNIA":"RO","RONDONIA":"RO","RORAIMA":"RR","SANTA CATARINA":"SC","SÃO PAULO":"SP","SAO PAULO":"SP",
    "SERGIPE":"SE","TOCANTINS":"TO"
}
UF_ID2SIGLA = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",
    29:"BA",31:"MG",32:"ES",33:"RJ",35:"SP",41:"PR",42:"SC",43:"RS",50:"MS",51:"MT",52:"GO",53:"DF"
}

COL_MAP = {
    "ext_id": {"id","uid"},
    "lat": {"latitude","lat"},
    "lon": {"longitude","lon"},
    "datetime_utc": {"data_hora_gmt","data_hora_utc","datahora_gmt","datahora","data_hora"},
    "sat": {"satellite","satélite","satelite"},
    "estado_nome": {"estado"},
    "estado_id": {"estado_id","uf_id","id_uf"},
    "municipio": {"municipio","município","cidade"},
    "municipio_ibge": {"municipio_id","id_municipio","codigo_ibge","cod_ibge","id_municip"},
    "bioma": {"bioma"},
    "confianca": {"frp_conf","confidence","confiança","frp"}
}

def _norm_colname(name: str) -> str:
    return name.strip().lower().replace(" ", "_")

def _pick(row: dict, keys: set[str]):
    for k in row.keys():
        if _norm_colname(k) in keys:
            return row[k]
    return None

def _to_uf_sigla(estado_nome, estado_id):
    sigla = None
    if estado_nome:
        sigla = UF_NOME2SIGLA.get(str(estado_nome).strip().upper())
    if not sigla and estado_id not in (None, "", "NULL"):
        try:
            sigla = UF_ID2SIGLA.get(int(estado_id))
        except Exception:
            pass
    return sigla

def parse_datetime(value):
    if not value: return None
    dt = dtparser.parse(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def coalesce(*vals, cast=float, default=None):
    for v in vals:
        if v is None or v == "": continue
        try:
            return cast(str(v).replace(",", "."))
        except Exception:
            continue
    return default

def row_to_doc(row: dict):
    lower = { _norm_colname(k): v for k,v in row.items() }
    def g(keys, default=None):
        return _pick(lower, COL_MAP[keys]) if keys in COL_MAP else default

    ext_id = g("ext_id")
    lat = coalesce(g("lat"), cast=float)
    lon = coalesce(g("lon"), cast=float)
    dt_utc = parse_datetime(g("datetime_utc"))
    sat = g("sat")
    estado_nome = g("estado_nome")
    estado_id = g("estado_id")
    uf_sigla = _to_uf_sigla(estado_nome, estado_id)
    municipio = g("municipio")
    municipio_ibge = coalesce(g("municipio_ibge"), cast=int, default=None)
    bioma = g("bioma")
    confianca = coalesce(g("confianca"), cast=float, default=None)

    if dt_utc is None or lat is None or lon is None:
        return None

    return {
        "ext_id": ext_id,  # GUID do INPE p/ dedupe
        "ts": dt_utc,
        "lat": lat,
        "lon": lon,
        "meta": {
            "uf": uf_sigla or estado_nome,  # prioriza sigla; fallback nome
            "municipio": municipio,
            "municipio_ibge": municipio_ibge,
            "bioma": bioma,
            "fonte": "INPE"
        },
        "sat": sat,
        "confianca": confianca,
        "ingest_ts": datetime.now(timezone.utc)
    }

def fetch_and_ingest(days: int = 7, batch_size: int = 2000, no_window: bool=False, debug: int=0):
    s = load_settings()
    if not s.inpe_csv_urls:
        print("ERRO: Configure INPE_CSV_URLS em configs/.env", file=sys.stderr)
        sys.exit(1)

    if not no_window:
        date_start, date_end = last_n_days_window(days)
        print(f"[WINDOW] {date_start.isoformat()} → {date_end.isoformat()} (UTC)")
    else:
        date_start = datetime(1970,1,1,tzinfo=timezone.utc)
        date_end = datetime(9999,1,1,tzinfo=timezone.utc)
        print("[WINDOW] desabilitada (no_window=True)")

    client = MongoClient(s.mongo_uri)
    db = client.get_database()
    col_ts = db.get_collection("raw_fires")              # time-series
    col_dedup = db.get_collection("dedup_fires_extid")   # normal com unique

    totals = {"read":0,"parsed":0,"out_of_window":0,"inserted":0,"skipped_dup":0}
    for url in s.inpe_csv_urls:
        print(f"[GET] {url}")
        r = httpx.get(url, timeout=120); r.raise_for_status()
        rd = csv.DictReader(io.StringIO(r.text), delimiter=s.csv_delimiter)
        print("[HEADERS]", rd.fieldnames)

        for row in tqdm(rd, desc="Processando"):
            totals["read"] += 1
            doc = row_to_doc(row)
            if doc is None:
                continue
            totals["parsed"] += 1
            if not (date_start <= doc["ts"] <= date_end):
                totals["out_of_window"] += 1
                continue

            ext_id = doc.get("ext_id")
            if not ext_id:
                # Sem ext_id: insere direto (podem ocorrer raros duplicados)
                col_ts.insert_one(doc)
                totals["inserted"] += 1
                continue

            # 1) tenta reservar a chave na dedup
            try:
                col_dedup.insert_one({"ext_id": ext_id})
            except DuplicateKeyError:
                totals["skipped_dup"] += 1
                continue

            # 2) grava no time-series
            col_ts.insert_one(doc)
            totals["inserted"] += 1

    client.close()
    print(f"[STATS] read={totals['read']} parsed={totals['parsed']} out_of_window={totals['out_of_window']} "
          f"inserted={totals['inserted']} skipped_dup={totals['skipped_dup']}")
    return totals

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--no-window", action="store_true")
    ap.add_argument("--debug", type=int, default=0)
    args = ap.parse_args()
    fetch_and_ingest(days=args.days, batch_size=args.batch, no_window=args.no_window, debug=args.debug)
