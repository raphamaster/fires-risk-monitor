# Fires Risk Monitor (INPE + Open-Meteo → MongoDB → Parquet → Power BI)

Pipeline de dados para monitorar **focos de queimadas** e **risco associado** (clima), com:
- **ETL em Python**
- **Armazenamento NoSQL** (MongoDB, coleções time-series)
- **Camada Gold** em **Parquet** (particionada)
- **Relatório Power BI** (3 páginas)

## Estrutura de Diretórios

```
fires-risk-monitor/
├─ docker/
│  └─ mongo/
│     └─ mongo-init.js
├─ etl/
│  ├─ inpe/
│  ├─ weather/
│  └─ common/
├─ data/
│  └─ gold/
├─ configs/
│  └─ .env.example
├─ .gitignore
├─ docker-compose.yml
└─ README.md
```

## Pré-requisitos

- **Docker** e **Docker Compose**
- **WSL Ubuntu** (22.04 recomendado)
- **mongosh** (Shell do MongoDB) para testes de conexão  
  Instalação recomendada (Ubuntu):  
  ```bash
  sudo apt-get update && sudo apt-get install -y curl gnupg
  curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \
    sudo gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
  source /etc/os-release
  echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu $UBUNTU_CODENAME/mongodb-org/7.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
  sudo apt-get update
  sudo apt-get install -y mongodb-mongosh
  ```

> Dica: em Ubuntu 22.04 (OpenSSL 3), se necessário use `mongodb-mongosh-shared-openssl3`.

## Subir o MongoDB

Na raiz do projeto:

```bash
docker compose up -d
docker compose logs -f mongo
```

No primeiro start, o script **mongo-init.js** cria:
- DB **`fires`**
- usuário **`etl_user` / `etl_pass`**
- coleções time-series: `raw_fires`, `raw_weather`
- coleção de referência: `ref_municipios`
- índices básicos

## Testes rápidos (conexão)

```bash
# Ping como root (DB admin)
mongosh "mongodb://root:example@localhost:27017/admin" --eval "db.runCommand({ ping: 1 })"

# Listar coleções como usuário de app (DB fires)
mongosh "mongodb://etl_user:etl_pass@localhost:27017/fires" --eval "db.getCollectionNames()"
```

Saída esperada: `raw_fires`, `raw_weather`, `ref_municipios`.

## Variáveis de Ambiente

Crie `configs/.env` a partir de `configs/.env.example`:

```
MONGO_URI=mongodb://etl_user:etl_pass@localhost:27017/fires?authSource=fires
```

## Próximas Etapas

1. **ETAPA 2 — Ingestão INPE (últimos 7 dias)**  
   - Script: `etl/inpe/fetch_fires.py`
   - Armazena documentos em `raw_fires`
2. **ETAPA 3 — Clima (Open-Meteo)**
3. **ETAPA 4 — Silver → Gold (Parquet)**
4. **ETAPA 5 — Power BI (3 páginas)**

Cada etapa terá passos curtos e verificações.

## Troubleshooting

- **Porta 27017 já em uso**  
  Pare processos/containers que usam essa porta, ou mude a porta do serviço no `docker-compose.yml`.

- **`mongosh: command not found`**  
  Refaça a instalação (ver bloco de pré-requisitos) e confirme com `mongosh --version`.

- **Permissão de escrita em `data/mongo`**  
  Garanta que a pasta exista e tenha permissão para o usuário do Docker Desktop/WSL.

## Licença
MIT
