# Workshop pg_lake — PostgreSQL × DuckDB × Apache Iceberg

> **Durée estimée** : 3h · **Niveau** : intermédiaire · **Prérequis** : SQL, bases Docker  
> **Stack** : pg_lake 3.2.2, PostgreSQL 18, DuckDB 1.3, MinIO (S3), Apache Iceberg v2

---

## Introduction

pg_lake transforme PostgreSQL en lakehouse autonome. Derrière une interface SQL standard, un moteur DuckDB exécute les requêtes analytiques en vectorisé, tandis que les données sont stockées en **Apache Iceberg** (Parquet + métadonnées) dans un object store S3 (ici MinIO).

Ce workshop couvre le cycle complet : création de tables Iceberg, ingestion bulk, partitionnement caché, requêtage analytique, compaction, export/import Parquet, interopérabilité multi-moteur et bonnes pratiques de production.

### Architecture du workshop

```
┌──────────────────────────────────────────────────────┐
│                  Client (psql)                       │
│                    port 5432                          │
└──────────┬───────────────────────────────────────────┘
           │
┌──────────▼───────────────────────────────────────────┐
│          pg_lake-postgres (PG 18)                     │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │ pg_lake_     │  │ pg_lake_     │  │ pg_lake_    │ │
│  │ iceberg      │  │ table (FDW)  │  │ copy        │ │
│  └──────┬──────┘  └──────┬───────┘  └──────┬──────┘ │
│         │    Unix socket  │                 │        │
│  ┌──────▼─────────────────▼─────────────────▼──────┐ │
│  │           pgduck-server (DuckDB 1.3)            │ │
│  └──────────────────────┬──────────────────────────┘ │
└─────────────────────────┼────────────────────────────┘
                          │ S3 API
┌─────────────────────────▼────────────────────────────┐
│                    MinIO (S3)                         │
│              testbucket/pglake/...                    │
│                  port 9000 / 9001                     │
└──────────────────────────────────────────────────────┘
```

---

## Module 0 — Mise en route

### 0.1 Lancement de l'environnement

```bash
docker compose up -d
docker compose ps   # vérifier les 3 services healthy
```

### 0.2 Connexion et vérification

```bash
docker exec -it pg_lake psql -U postgres
```

```sql
-- Vérifier que pg_lake est opérationnel
CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;

-- Smoke test : créer et supprimer une table Iceberg
CREATE TABLE smoke_test (id int) USING iceberg;
INSERT INTO smoke_test VALUES (1);
SELECT * FROM smoke_test;
DROP TABLE smoke_test;

-- Vérifier le moteur DuckDB sous-jacent
-- (via la connexion pgduck sur le socket Unix)
```

### 0.3 Accès à la console MinIO

Ouvrir http://localhost:9001 — identifiants `minioadmin` / `minioadmin`.  
Naviguer dans `testbucket` pour voir la structure Iceberg se construire au fil du workshop.

---

## Module 1 — Tables Iceberg : fondamentaux

### 1.1 Créer une table Iceberg simple

```sql
CREATE TABLE logs (
    log_time   timestamptz NOT NULL,
    level      text        NOT NULL,
    service    text,
    message    text
) USING iceberg;

INSERT INTO logs VALUES
    (now(), 'INFO',  'auth',    'User login successful'),
    (now(), 'ERROR', 'payment', 'Timeout on gateway'),
    (now(), 'WARN',  'auth',    'Rate limit approaching');

SELECT * FROM logs;
```

**Point clé** : `USING iceberg` remplace le stockage heap de PostgreSQL. Les données sont écrites en Parquet dans MinIO, les métadonnées suivent le protocole Iceberg v2.

### 1.2 Explorer le catalogue Iceberg

```sql
-- pg_lake expose un catalogue interne
SELECT table_name, metadata_location
FROM iceberg_tables;
```

Aller vérifier dans MinIO le chemin affiché : on y trouvera le `metadata.json`, les manifests et les fichiers Parquet.

### 1.3 Types PostgreSQL supportés

```sql
CREATE TABLE type_demo (
    id          serial,
    name        text,
    score       numeric(10,2),
    tags        text[],
    metadata    jsonb,
    created_at  timestamptz DEFAULT now()
) USING iceberg;

INSERT INTO type_demo (name, score, tags, metadata) VALUES
    ('Alice', 95.5, ARRAY['vip','eu'], '{"plan":"premium"}'),
    ('Bob',   72.0, ARRAY['us'],       '{"plan":"free"}');

SELECT * FROM type_demo;
DROP TABLE type_demo;
```

---

## Module 2 — Partitionnement caché (Hidden Partitioning)

### 2.1 Concept

Contrairement au partitionnement déclaratif de PostgreSQL (qui crée des tables enfants), Iceberg gère le partitionnement dans les métadonnées. On déclare les expressions, Iceberg organise les fichiers automatiquement.

### 2.2 Créer la table partitionnée

```sql
CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    user_id     bigint      NOT NULL,
    region      text        NOT NULL,
    event_type  text,
    payload     jsonb
) USING iceberg
WITH (
    partition_by = 'day(event_time), bucket(32, user_id)'
);
```

**Expressions supportées** :

| Expression | Description | Exemple |
|---|---|---|
| `year(col)` | Partition par année | `year(event_time)` |
| `month(col)` | Partition par mois | `month(event_time)` |
| `day(col)` | Partition par jour | `day(event_time)` |
| `hour(col)` | Partition par heure | `hour(event_time)` |
| `bucket(N, col)` | Hachage modulo N | `bucket(32, user_id)` |
| `truncate(N, col)` | Troncature à N chars | `truncate(3, region)` |

### 2.3 Contrainte des file descriptors

Chaque partition ouverte consomme un file descriptor PostgreSQL. Avec `day × bucket(32)`, on peut atteindre **jours × 32** fichiers simultanés, ce qui dépasse la limite `maxAllocatedDescs` (~330 par défaut).

**Solutions** :

1. **Relever la limite** : `ALTER SYSTEM SET max_files_per_process = 10000;`
2. **Batcher les inserts** par tranches temporelles
3. **Utiliser une table staging** puis un INSERT trié

---

## Module 3 — Ingestion bulk (1 million de lignes)

### 3.1 Stratégie staging + INSERT trié

L'idée : générer les données dans une table heap classique (rapide, pas de partitionnement), puis insérer dans la table Iceberg en une ou quelques transactions triées par clés de partition.

```sql
-- Étape 1 : Générer les données en staging (table heap, ~15 secondes)
CREATE TEMP TABLE staging_events AS
WITH
users AS (
    SELECT
        (random() * 49999 + 1)::bigint AS user_id,
        greatest(1, (50 / (row_number() OVER () ^ 0.5 + 1) * 200)::int) AS weight
    FROM generate_series(1, 5000)
),
expanded AS (
    SELECT u.user_id, u.weight, g.i
    FROM users u
    CROSS JOIN LATERAL generate_series(1, u.weight) AS g(i)
    LIMIT 1000000
),
region_pick AS (
    SELECT unnest AS region, row_number() OVER () AS rn
    FROM unnest(ARRAY[
        'eu-west-1','eu-west-1','eu-west-1','eu-west-1',
        'eu-central-1','eu-central-1','eu-central-1',
        'us-east-1','us-east-1',
        'us-west-2',
        'ap-southeast-1',
        'ap-northeast-1',
        'sa-east-1',
        'af-south-1'
    ])
)
SELECT
    now() - (random() * interval '90 days')
      + make_interval(hours => ((random() + random()) * 6 + 7)::int)
      AS event_time,
    e.user_id,
    (SELECT region FROM region_pick ORDER BY random() LIMIT 1) AS region,
    CASE
        WHEN random() < 0.45 THEN 'page_view'
        WHEN random() < 0.65 THEN 'click'
        WHEN random() < 0.80 THEN 'signup'
        WHEN random() < 0.92 THEN 'purchase'
        WHEN random() < 0.97 THEN 'logout'
        ELSE                      'error'
    END AS event_type,
    jsonb_build_object(
        'session_id',  md5(e.user_id::text || e.i::text || random()::text),
        'duration_ms', CASE
            WHEN e.weight > 100 THEN (random() * 60000 + 5000)::int
            ELSE (random() * 15000 + 500)::int
        END,
        'device',      CASE
            WHEN random() < 0.55 THEN 'mobile'
            WHEN random() < 0.85 THEN 'desktop'
            ELSE                      'tablet'
        END,
        'app_version', CASE
            WHEN random() < 0.6 THEN '3.'
            WHEN random() < 0.9 THEN '2.'
            ELSE                     '1.'
        END || (floor(random() * 10))::int || '.' || (floor(random() * 20))::int,
        'is_premium',  (e.weight > 50 AND random() > 0.4),
        'referrer',    (ARRAY[
            'google','direct','email','social','affiliate','partner'
        ])[1 + (floor(random() * 6))::int],
        'error_code',  CASE WHEN random() > 0.95
            THEN (400 + (floor(random()*5))*100)::int END
    ) AS payload
FROM expanded e;
```

### 3.2 Vérification du staging

```sql
SELECT count(*) FROM staging_events;

SELECT event_type,
       count(*),
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM staging_events
GROUP BY 1
ORDER BY 2 DESC;
```

### 3.3 Insertion par tranches (contournement file descriptors)

```sql
-- Index pour accélérer les range scans
CREATE INDEX ON staging_events (event_time);

-- Insertion par tranches de 10 jours (~320 partitions max par batch)
DO $$
DECLARE
    d_start date;
    d_end   date;
BEGIN
    FOR i IN 0..8 LOOP
        d_start := current_date - 89 + (i * 10);
        d_end   := d_start + 10;

        INSERT INTO events
        SELECT event_time, user_id, region, event_type, payload
        FROM staging_events
        WHERE event_time >= d_start AND event_time < d_end
        ORDER BY date_trunc('day', event_time),
                 abs(hashint8(user_id)) % 32;

        COMMIT;
        RAISE NOTICE 'Batch % terminé : % → %', i, d_start, d_end;
    END LOOP;
END $$;

DROP TABLE staging_events;
```

---

## Module 4 — Compaction et maintenance

### 4.1 Le problème des petits fichiers

Avec 1M de lignes réparties sur 90 jours × 32 buckets × 9 batchs, on obtient potentiellement des milliers de fichiers Parquet de quelques centaines de lignes. Chaque fichier a un overhead (footer Parquet, entrée manifest).

```sql
-- Compter les fichiers Iceberg (si exposé)
-- SELECT count(*) FROM events$files;
-- SELECT * FROM events$manifests;
```

Vérifier dans MinIO : naviguer dans le chemin `testbucket/pglake/postgres/public/events/` et observer la taille des fichiers `.parquet`.

### 4.2 VACUUM — Compaction Iceberg

```sql
-- Compaction : fusionne les petits fichiers en fichiers plus grands
VACUUM events;
```

VACUUM sur une table Iceberg dans pg_lake effectue :

1. **Compaction des données** : fusion des petits fichiers Parquet en fichiers plus grands (cible ~256 MB)
2. **Nettoyage des métadonnées** : suppression des fichiers manifest devenus inutiles
3. **Expiration des snapshots** : suppression des anciens snapshots et des fichiers de données orphelins (rétention par défaut : 10 jours)

### 4.3 Vérification post-compaction

Retourner dans MinIO et comparer le nombre et la taille des fichiers. On devrait observer beaucoup moins de fichiers, chacun nettement plus volumineux.

### 4.4 Bonnes pratiques compaction

```sql
-- Automatisation avec pg_cron (si disponible)
-- SELECT cron.schedule('vacuum_events', '0 3 * * *', 'VACUUM events');

-- Pour vacuumer toutes les tables Iceberg :
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN SELECT table_name FROM iceberg_tables LOOP
        EXECUTE format('VACUUM %I', tbl);
        RAISE NOTICE 'VACUUM terminé pour %', tbl;
    END LOOP;
END $$;
```

---

## Module 5 — Requêtes analytiques

### 5.1 Agrégations temporelles

```sql
-- Volume quotidien avec moyenne mobile 7 jours
SELECT
    jour,
    nb,
    round(avg(nb) OVER (ORDER BY jour ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS avg_7j
FROM (
    SELECT date_trunc('day', event_time)::date AS jour, count(*) AS nb
    FROM events
    GROUP BY 1
) daily
ORDER BY 1;
```

### 5.2 Extraction JSONB

```sql
-- Durée moyenne de session par device
SELECT
    payload->>'device'                          AS device,
    round(avg((payload->>'duration_ms')::int))  AS avg_duration_ms,
    count(*)                                    AS nb
FROM events
GROUP BY 1
ORDER BY 2 DESC;

-- Taux de premium par région
SELECT
    region,
    count(*) FILTER (WHERE (payload->>'is_premium')::boolean) AS premium,
    count(*)                                                   AS total,
    round(100.0 * count(*) FILTER (WHERE (payload->>'is_premium')::boolean)
          / count(*), 1)                                       AS pct_premium
FROM events
GROUP BY 1
ORDER BY 4 DESC;
```

### 5.3 Funnel d'acquisition

```sql
WITH user_events AS (
    SELECT
        user_id,
        bool_or(event_type = 'page_view') AS has_view,
        bool_or(event_type = 'click')     AS has_click,
        bool_or(event_type = 'signup')    AS has_signup,
        bool_or(event_type = 'purchase')  AS has_purchase
    FROM events
    GROUP BY 1
)
SELECT
    count(*) FILTER (WHERE has_view)     AS viewers,
    count(*) FILTER (WHERE has_click)    AS clickers,
    count(*) FILTER (WHERE has_signup)   AS signups,
    count(*) FILTER (WHERE has_purchase) AS buyers
FROM user_events;
```

### 5.4 Heatmap temporelle (heures × jours)

```sql
SELECT
    extract(dow FROM event_time)::int  AS jour_semaine,
    extract(hour FROM event_time)::int AS heure,
    count(*)                           AS nb
FROM events
GROUP BY 1, 2
ORDER BY 1, 2;
```

### 5.5 Partition pruning

```sql
-- Ce filtre devrait ne scanner qu'1 jour × 1 bucket
EXPLAIN ANALYZE
SELECT *
FROM events
WHERE event_time >= '2026-03-15' AND event_time < '2026-03-16'
  AND user_id = 42;
```

Observer dans le plan : `Data Files Skipped` indique le nombre de fichiers évités grâce au pruning.

---

## Module 6 — Gestion de la rétention (DELETE par partition)

### 6.1 Suppression rapide par partition entière

Quand un `DELETE` correspond à une partition complète, pg_lake supprime les fichiers de données directement (opération sur les métadonnées, pas de scan).

```sql
-- Supprimer les données de plus de 60 jours
EXPLAIN (VERBOSE)
DELETE FROM events
WHERE event_time < now() - interval '60 days';

-- Exécuter
DELETE FROM events
WHERE event_time < now() - interval '60 days';

-- Nettoyer les fichiers orphelins
VACUUM events;
```

### 6.2 Vérification

```sql
SELECT
    min(event_time)::date AS premiere_date,
    max(event_time)::date AS derniere_date,
    count(*)              AS nb_rows
FROM events;
```

---

## Module 7 — Import / Export avec COPY

### 7.1 Export vers Parquet dans MinIO

```sql
-- Exporter la table complète
COPY (SELECT * FROM events)
TO 's3://testbucket/exports/events_full.parquet';

-- Exporter un sous-ensemble
COPY (
    SELECT event_time, user_id, region, event_type
    FROM events
    WHERE region = 'eu-west-1'
)
TO 's3://testbucket/exports/events_eu_west.parquet';

-- Exporter en CSV
COPY (SELECT * FROM events LIMIT 1000)
TO 's3://testbucket/exports/events_sample.csv'
WITH (format 'csv', header);
```

### 7.2 Import depuis Parquet

```sql
-- Créer une table Iceberg et charger depuis Parquet
CREATE TABLE events_imported (
    event_time  timestamptz,
    user_id     bigint,
    region      text,
    event_type  text
) USING iceberg;

COPY events_imported
FROM 's3://testbucket/exports/events_eu_west.parquet';

SELECT count(*) FROM events_imported;
```

### 7.3 Créer une table directement depuis un fichier

```sql
-- load_from : inférence de schéma automatique
CREATE TABLE events_from_file ()
USING iceberg
WITH (load_from = 's3://testbucket/exports/events_full.parquet');

\d events_from_file
SELECT count(*) FROM events_from_file;
```

---

## Module 8 — Foreign tables (query in place)

### 8.1 Requêter des fichiers Parquet sans les importer

```sql
-- Créer une foreign table : pg_lake infère les colonnes
CREATE FOREIGN TABLE events_parquet ()
SERVER pg_lake
OPTIONS (path 's3://testbucket/exports/events_eu_west.parquet');

-- Inspecter le schéma inféré
\d events_parquet

-- Requêter directement
SELECT event_type, count(*)
FROM events_parquet
GROUP BY 1
ORDER BY 2 DESC;
```

### 8.2 Requêter un répertoire entier

```sql
-- Wildcard sur tous les fichiers Parquet du dossier exports
CREATE FOREIGN TABLE all_exports ()
SERVER pg_lake
OPTIONS (path 's3://testbucket/exports/*.parquet');

SELECT count(*) FROM all_exports;
```

### 8.3 Joindre heap + Iceberg + foreign

```sql
-- Table heap classique
CREATE TABLE user_profiles (
    user_id  bigint PRIMARY KEY,
    name     text,
    tier     text
);

INSERT INTO user_profiles VALUES
    (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (42, 'Charlie', 'platinum');

-- Jointure cross-engine : heap × Iceberg
SELECT
    u.name,
    u.tier,
    count(e.*) AS nb_events
FROM user_profiles u
JOIN events e ON e.user_id = u.user_id
GROUP BY 1, 2
ORDER BY 3 DESC;
```

Ce type de jointure cross-engine (heap + Iceberg) est transactionnel — c'est un des atouts majeurs de pg_lake.

---

## Module 9 — Interopérabilité multi-moteur

### 9.1 Le catalogue Iceberg de pg_lake

pg_lake agit comme son propre catalogue Iceberg. Les métadonnées sont accessibles via :

```sql
SELECT table_name, metadata_location FROM iceberg_tables;
```

Ce `metadata_location` pointe vers le fichier `metadata.json` dans MinIO. Tout moteur Iceberg compatible (Spark, PyIceberg, Trino, Dremio) peut lire ces tables.

### 9.2 Accès depuis PyIceberg (exemple)

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "pg_lake",
    **{
        "uri": "postgresql+psycopg2://postgres@localhost:5432/postgres",
        "warehouse": "s3://testbucket/pglake",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
    }
)

# Lister les tables
print(catalog.list_tables("public"))

# Charger et scanner
table = catalog.load_table("public.events")
df = table.scan().to_pandas()
print(df.head())
```

### 9.3 Accès depuis Spark

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.pg_lake", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.pg_lake.type", "jdbc") \
    .config("spark.sql.catalog.pg_lake.uri",
            "jdbc:postgresql://localhost:5432/postgres") \
    .config("spark.sql.catalog.pg_lake.warehouse", "s3://testbucket/pglake") \
    .getOrCreate()

spark.sql("SELECT count(*) FROM pg_lake.public.events").show()
```

### 9.4 Accès depuis Dremio

Dans Dremio, configurer une source de type "Iceberg Catalog" pointant vers le metadata_location ou un catalogue REST/JDBC. Les tables créées par pg_lake sont lisibles nativement — c'est la promesse de l'open table format.

---

## Module 10 — Bonnes pratiques et récapitulatif

### Stratégie de partitionnement

| Volume | Recommandation |
|---|---|
| < 10 Go | Pas de partition ou `month(col)` seul |
| 10–100 Go | `day(col)` |
| 100+ Go | `day(col), bucket(N, col)` avec N adapté |

**Règle du pouce** : viser des fichiers Parquet de 128 MB à 512 MB après compaction.

### Ingestion

- Éviter les INSERT unitaires → accumuler dans une table staging heap puis INSERT bulk
- Trier par clés de partition avant insertion pour minimiser les fichiers ouverts simultanément
- VACUUM régulièrement après les batchs d'insertion

### Requêtes

- Toujours inclure la colonne de partition dans les filtres (`WHERE event_time >= ...`)
- Exploiter `EXPLAIN ANALYZE` pour vérifier le partition pruning (`Data Files Skipped`)
- Les jointures heap × Iceberg sont transactionnelles

### Maintenance

- `VACUUM` pour compaction + expiration des snapshots
- Surveiller le nombre de fichiers et leur taille dans MinIO
- Planifier les VACUUM avec `pg_cron` en production

### Export / Interopérabilité

- `COPY ... TO` pour exporter en Parquet, CSV, JSON vers S3
- `load_from` pour créer des tables Iceberg depuis des fichiers existants
- Le catalogue interne est accessible depuis Spark, PyIceberg, Dremio, Trino

---

## Nettoyage

```sql
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS events_imported;
DROP TABLE IF EXISTS events_from_file;
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS user_profiles;
DROP FOREIGN TABLE IF EXISTS events_parquet;
DROP FOREIGN TABLE IF EXISTS all_exports;
```

```bash
docker compose down -v
```

---

## Pour aller plus loin

- [PG_LAKE : PostgreSQL + Data Lakehouse](https://www.synaltic.fr/blog/pg_lake-quand-postgresql-rencontre-le-data-lakehouse-apache-iceberg)
- [Documentation pg_lake — Iceberg Tables](https://github.com/Snowflake-Labs/pg_lake/blob/main/docs/iceberg-tables.md)
- [Blog Snowflake — Introducing pg_lake](https://www.snowflake.com/en/engineering-blog/pg-lake-postgres-lakehouse-integration/)
- [Blog Snowflake — Time Series avec pg_lake + pg_incremental](https://www.snowflake.com/en/engineering-blog/postgres-time-series-iceberg/)
- [Apache Iceberg Spec v2](https://iceberg.apache.org/spec/)

---

*Workshop conçu par Synaltic — les artisans de la donnée*  
*Contact : [https://www.synaltic.fr/contact](https://www.synaltic.fr/contact)*
*Site : [https://www.synaltic.fr](https://www.synaltic.fr)*
