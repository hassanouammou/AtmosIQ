
import os
import json
from pathlib import Path

from confluent_kafka import Consumer
from minio import Minio
from dotenv import load_dotenv

# 1. Charger les identifiants MinIO
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# 2. Connexion à MinIO
minio_client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False
)

BUCKET = "bronze"

if not minio_client.bucket_exists(BUCKET):
    raise RuntimeError("Le bucket bronze n'existe pas")

# 3. Connexion à Kafka
consumer = Consumer({
    "bootstrap.servers": "localhost:9094",
    "group.id": "atmosiq-storage-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["meteo"])

print("En attente des mesures météo...")

# 4. Lire les messages Kafka
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Erreur Kafka :", msg.error())
            continue

        try:
            data = json.loads(msg.value())
            station = data["station_id"]

            # Nom déterministe pour éviter les doublons
            object_name = (
                f"meteo/{station}/"
                f"partition_{msg.partition()}_offset_{msg.offset()}.json"
            )

            # 5. Enregistrer dans MinIO
            from io import BytesIO

            contenu = msg.value()

            minio_client.put_object(
                BUCKET,
                object_name,
                BytesIO(contenu),
                length=len(contenu),
                content_type="application/json"
            )

            # 6. Confirmer le traitement dans Kafka
            consumer.commit(message=msg, asynchronous=False)

            print(f"Stocké : bronze/{object_name}")

        except (ValueError, KeyError, TypeError) as e:
            print("Message invalide :", e)
        except Exception as e:
            print("Erreur de stockage :", e)

except KeyboardInterrupt:
    print("\nArrêt du consommateur")

finally:
    consumer.close()
