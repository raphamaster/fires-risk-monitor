import pandas as pd
from pathlib import Path
import re

raw = Path("data/ref/municipios_raw.csv")
out = Path("data/ref/municipios.csv")

# Lê o CSV (vírgula), mantendo os nomes originais
df = pd.read_csv(raw, sep=",", dtype=str)

# Normaliza nomes de colunas (remove acentos, espaços extras e pontuação)
def norm(s):
    s = s.strip().lower()
    s = s.replace("á","a").replace("à","a").replace("ã","a").replace("â","a")
    s = s.replace("é","e").replace("ê","e").replace("í","i").replace("ó","o").replace("ô","o").replace("õ","o").replace("ú","u").replace("ç","c")
    s = re.sub(r"[^a-z0-9_]+", "_", s)  # troca qualquer coisa por _
    s = re.sub(r"_+", "_", s).strip("_")
    return s

cols_map = {c: norm(c) for c in df.columns}
df.rename(columns=cols_map, inplace=True)

# Esperados a partir do seu layout:
# uf_sigla -> "uf_sigla"
# COD. UF  -> "cod_uf"
# codigo_ibge (5 dígitos do município) -> "codigo_ibge"
# nome_municipio -> "nome_municipio"
# populacao_estimada (com espaço no fim) -> vira "populacao_estimada"
req = ["uf_sigla","cod_uf","codigo_ibge","nome_municipio","populacao_estimada"]
missing = [c for c in req if c not in df.columns]
if missing:
    raise SystemExit(f"Colunas obrigatorias ausentes no CSV bruto: {missing}")

# Monta o codigo_ibge completo: UF(2) + MUN(5) => 7 dígitos (ex.: 11 + 00015 => 1100015)
def to_int(s):
    s = (s or "").strip()
    s = re.sub(r"\D", "", s)  # só dígitos
    return int(s) if s else 0

df["cod_uf_int"] = df["cod_uf"].apply(to_int)
df["mun5_int"]   = df["codigo_ibge"].apply(to_int)

df["codigo_ibge_full"] = df.apply(lambda r: int(f"{r['cod_uf_int']:02d}{r['mun5_int']:05d}"), axis=1)

# População: remove separador de milhar e converte para int
def parse_pop(s):
    s = (str(s) if s is not None else "").strip()
    s = s.replace(".", "").replace(",", ".")  # remove milhares; se viesse dec, vira ponto
    try:
        return int(float(s))
    except:
        return 0

df["populacao"] = df["populacao_estimada"].apply(parse_pop)

# Saída padronizada
out_df = pd.DataFrame({
    "codigo_ibge": df["codigo_ibge_full"].astype(int),
    "municipio": df["nome_municipio"].astype(str).str.strip(),
    "uf_sigla": df["uf_sigla"].astype(str).str.upper().str.strip(),
    "populacao": df["populacao"].astype(int)
})

out.parent.mkdir(parents=True, exist_ok=True)
out_df.to_csv(out, index=False)
print("Gerado:", out, " | Linhas:", len(out_df))
print(out_df.head(5))
