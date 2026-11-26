
from pathlib import Path
from datetime import datetime, timezone
from minio import Minio
from dotenv import load_dotenv
import os

# 1. Localiser les fichiers
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# 2. Se connecter à MinIO
client = Minio(
    "localhost:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False
)

# 3. Définir le fichier et le bucket
bucket = "bronze"
file_path = BASE_DIR / "test_meteo.json"

# 4. Vérifier que le bucket existe
if not client.bucket_exists(bucket):
    raise RuntimeError(f"Bucket introuvable : {bucket}")

# 5. Générer un nom unique
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
object_name = f"meteo/ST01/{timestamp}.json"

# 6. Envoyer le fichier
client.fput_object(
    bucket,
    object_name,
    str(file_path),
    content_type="application/json"
)

print("Upload réussi !")
print(f"Destination : {bucket}/{object_name}")
