-- Execute automatiquement au premier demarrage de Postgres (base "meteo")
CREATE DATABASE airflow;

-- Tables de la zone Gold exposees a Grafana.
-- A ADAPTER avec le Membre 4 (noms et colonnes reels des tables Gold).
CREATE TABLE IF NOT EXISTS stations (
    station_id TEXT PRIMARY KEY,
    nom        TEXT NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,
    lon        DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_mesures_horaires (
    station_id  TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION,
    pressure    DOUBLE PRECISION,
    PRIMARY KEY (station_id, ts)
);

CREATE TABLE IF NOT EXISTS gold_previsions (
    station_id  TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    temp_prevue DOUBLE PRECISION,
    temp_reelle DOUBLE PRECISION,
    PRIMARY KEY (station_id, ts)
);

CREATE TABLE IF NOT EXISTS gold_capteurs_statut (
    station_id      TEXT PRIMARY KEY,
    statut          TEXT NOT NULL,      -- 'OK' ou 'DEFAILLANT'
    raison          TEXT,
    derniere_mesure TIMESTAMPTZ
);