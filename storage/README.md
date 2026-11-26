@'
# AtmosIQ - Data Storage

## Architecture
MQTT -> Kafka -> MinIO Bronze -> Spark/Hudi Silver -> Spark/Hudi Gold

## Technologies
- Docker
- MinIO
- Apache Kafka
- Apache Spark 3.5.x
- Apache Hudi 1.0.2

## Buckets
- bronze: raw weather data (JSON)
- silver: validated weather data (Hudi)
- gold: aggregated weather statistics (Hudi)

## Scripts
- kafka_to_minio.py: Kafka to Bronze
- spark_to_hudi.py: Bronze to Silver
- spark_gold.py: Silver to Gold

## Configuration
Create storage/.env with:
- MINIO_ROOT_USER
- MINIO_ROOT_PASSWORD

Never commit .env files.

## Tested Results
- Bronze: 21 records
- Silver: 21 records
- Gold: 3 weather stations

## Current Limitations
- Spark jobs are executed manually.
- Integration with other team modules is pending.
'@ | Set-Content -Path storage/README.md -Encoding utf8