# Membre 1 - Rejeu des données météo réelles -> MQTT

## 1. Télécharger les données dans ./data
Intel Berkeley (http://db.csail.mit.edu/labdata/labdata.html) :
    cd data && wget http://db.csail.mit.edu/labdata/data.txt.gz && gunzip data.txt.gz
    (+ mote_locs.txt de la même page, optionnel : positions des capteurs)
NOAA ISD-Lite : fichiers  USAF-WBAN-ANNEE.gz  (+ isd-history.csv pour lat/lon)

## 2. Lancer
    docker compose up -d --build                       # Intel (défaut)
    MODE=noaa DATA_FILE=/data/010010-99999-2023.gz,/data/603030-99999-2023.gz docker compose up -d
    MODE=synthetic docker compose up -d                # secours sans fichier
    docker compose logs -f simulator

## 3. Vérifier
    docker exec -it mosquitto mosquitto_sub -t "meteo/#" -v

## Contrat de données (topic meteo/stations/<station_id>, QoS 1)
station_id (texte), station_name, latitude, longitude, ts (date de la mesure d'origine, UTC),
ingest_ts (date d'envoi), temperature, humidity, pressure, wind_speed, battery_v, light
Les valeurs absentes de la source sont null (pressure/wind_speed absents d'Intel, light/battery absents de NOAA).
