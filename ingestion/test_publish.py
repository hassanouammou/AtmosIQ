import json
import random
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect("localhost", 1883)
client.loop_start()

for i in range(20):
    station = f"ST0{random.randint(1, 3)}"
    mesure = {
        "station_id": station,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(15, 30), 1),
        "humidity": round(random.uniform(40, 80), 1),
    }
    client.publish(f"meteo/{station}", json.dumps(mesure))
    print("publie :", mesure)
    time.sleep(1)

client.publish("meteo/ST99", "ceci n'est pas du json")
time.sleep(1)
client.loop_stop()
