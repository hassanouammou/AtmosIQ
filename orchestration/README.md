# Orchestration & Dashboard (Airflow + Grafana)

| Service    | URL                    | Identifiants   |
|------------|------------------------|----------------|
| Airflow    | http://localhost:8080  | admin / admin  |
| Grafana    | http://localhost:3000  | admin / admin  |
| PostgreSQL | localhost:5432         | atmos / atmos (base `meteo`) |

## Lancer
    cd orchestration
    docker compose up -d

Premier demarrage : attendre 1 a 2 minutes qu'Airflow soit pret.

## Donnees fictives (pour construire le dashboard)
    docker exec atmos-postgres psql -U atmos -d meteo -f /sql/seed_fake.sql

## Contenu
- `dags/meteo_daily.py` : DAG quotidien (taches Spark encore en placeholder)
- `sql/init.sql` : tables Gold lues par Grafana (a aligner avec le Membre 4)
- `sql/seed_fake.sql` : donnees de test
- `grafana/provisioning/` : source de donnees Grafana (Postgres)

## Arreter
    docker compose down        # garde les donnees
    docker compose down -v     # supprime aussi les volumes