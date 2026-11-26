import json
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = "localhost"
MQTT_PORT = 1883
KAFKA_SERVER = "localhost:9094"
TOPIC = "meteo"

producer = Producer({"bootstrap.servers": KAFKA_SERVER})
compteur = 0


def on_connect(client, userdata, flags, reason_code, properties):
    print("Connecte a MQTT, code :", reason_code)
    client.subscribe("meteo/#")


def on_message(client, userdata, msg):
    global compteur
    try:
        data = json.loads(msg.payload)
        station = data["station_id"]
    except (ValueError, KeyError, TypeError):
        print("Message invalide ignore :", msg.payload)
        return
    producer.produce(TOPIC, key=station, value=msg.payload)
    producer.poll(0)
    compteur += 1
    print(f"[{compteur}] envoye a Kafka, station {station}")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_HOST, MQTT_PORT)
client.loop_forever()
