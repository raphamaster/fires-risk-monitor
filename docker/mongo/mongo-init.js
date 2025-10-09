// roda como root no primeiro start
(function () {
  // cria DB de aplicação e usuário de ETL
  const appDbName = "fires";
  const appUser = "etl_user";
  const appPwd  = "etl_pass";

  const admin = db.getSiblingDB("admin");
  const appdb = db.getSiblingDB(appDbName);

  // cria usuário de aplicação (readWrite)
  appdb.createUser({
    user: appUser,
    pwd:  appPwd,
    roles: [{ role: "readWrite", db: appDbName }]
  });

  // coleções time-series
  appdb.createCollection("raw_fires", {
    timeseries: { timeField: "ts", metaField: "meta", granularity: "hours" }
  });

  appdb.createCollection("raw_weather", {
    timeseries: { timeField: "ts", metaField: "meta", granularity: "hours" }
  });

  // referência estática/dimensional
  appdb.createCollection("ref_municipios");

  // índices úteis
  appdb.raw_fires.createIndex({ "meta.uf": 1, ts: -1 });
  appdb.raw_fires.createIndex({ ts: -1 });
  // Caso use GeoJSON, preferir campo 'geom': {type:"Point", coordinates:[lon,lat]}
  // appdb.raw_fires.createIndex({ geom: "2dsphere" });

  appdb.raw_weather.createIndex({ "meta.municipio_ibge": 1, ts: -1 });
  appdb.raw_weather.createIndex({ ts: -1 });

  appdb.ref_municipios.createIndex({ codigo_ibge: 1 }, { unique: true });

  print("Mongo init OK: DB fires, usuário etl_user e coleções criadas.");
})();
