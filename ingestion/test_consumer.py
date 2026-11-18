from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9094",
    "group.id": "test-lecture",
    "auto.offset.reset": "earliest",
})
c.subscribe(["meteo"])
print("En attente de messages Kafka...")
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur :", msg.error())
            continue
        cle = msg.key().decode() if msg.key() else "-"
        print(f"partition {msg.partition()} | cle {cle} | {msg.value().decode()}")
except KeyboardInterrupt:
    pass
finally:
    c.close()
