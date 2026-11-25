"""DAG quotidien AtmosIQ : synthese et previsions meteo.

Pour l'instant les taches Spark sont des PLACEHOLDERS (echo).
Quand le Membre 4 livre ses scripts, remplacer les commandes par les vrais
spark-submit (voir les commentaires), sans changer la structure du DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "atmosiq",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="meteo_daily",
    description="Silver -> Gold -> previsions -> chargement Postgres pour Grafana",
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["atmosiq", "meteo"],
) as dag:

    check_bronze = BashOperator(
        task_id="check_bronze",
        bash_command='echo "TODO: verifier que MinIO Bronze contient des donnees recentes"',
    )

    job_silver = BashOperator(
        task_id="job_silver",
        # TODO: spark-submit /opt/spark/jobs/job_silver.py
        bash_command='echo "TODO: spark-submit job_silver.py (nettoyage + controle qualite)"',
    )

    job_gold = BashOperator(
        task_id="job_gold",
        # TODO: spark-submit /opt/spark/jobs/job_gold.py
        bash_command='echo "TODO: spark-submit job_gold.py (syntheses + capteurs defaillants)"',
    )

    job_forecast = BashOperator(
        task_id="job_forecast",
        # TODO: spark-submit /opt/spark/jobs/job_forecast.py
        bash_command='echo "TODO: spark-submit job_forecast.py (MLlib : previsions temperature)"',
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command='echo "TODO: verifier que le Gold est non vide et a jour"',
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command='echo "TODO: charger le Gold dans PostgreSQL pour Grafana"',
    )

    check_bronze >> job_silver >> job_gold >> job_forecast >> quality_check >> load_postgres