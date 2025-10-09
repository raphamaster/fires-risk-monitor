import os
from dataclasses import dataclass

@dataclass
class Settings:
    mongo_uri: str
    inpe_csv_urls: list[str]
    csv_encoding: str
    csv_delimiter: str
    csv_decimal: str
    csv_date_tz: str

def load_settings(env_path: str = "configs/.env") -> Settings:
    # leitura simples de .env sem dependências
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#"): 
                    continue
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

    return Settings(
        mongo_uri=os.environ.get("MONGO_URI", "mongodb://etl_user:etl_pass@localhost:27017/fires?authSource=fires"),
        inpe_csv_urls=[u.strip() for u in os.environ.get("INPE_CSV_URLS","").split(",") if u.strip()],
        csv_encoding=os.environ.get("CSV_ENCODING","latin1"),
        csv_delimiter=os.environ.get("CSV_DELIMITER",";"),
        csv_decimal=os.environ.get("CSV_DECIMAL","."),
        csv_date_tz=os.environ.get("CSV_DATE_TZ","UTC"),
    )
