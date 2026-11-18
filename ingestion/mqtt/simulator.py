"""Simulateur de stations météo -> MQTT.

Chaque station publie un message JSON sur  <TOPIC_PREFIX>/<station_id>
Contrat de données (à partager avec toute l'équipe) :
  station_id, station_name, latitude, longitude, ts (ISO 8601 UTC),
  temperature (°C), humidity (%), pressure (hPa), wind_speed (km/h), battery_v (V)
Des pannes de capteurs sont injectées volontairement (valeurs aberrantes,
valeurs figées, valeurs manquantes) pour que le contrôle qualité en aval
ait quelque chose à détecter.
"""
import json
import math
import os
import random
import signal
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

from replay import intel_records, noaa_records

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "meteo/stations")
NB_STATIONS = int(os.getenv("NB_STATIONS", "8"))
INTERVAL = float(os.getenv("INTERVAL_SECONDS", "2"))
SIM_STEP = float(os.getenv("SIM_STEP_MINUTES", "10"))
FAULT_RATE = float(os.getenv("FAULT_RATE", "0.03"))

# Source des données : "intel" | "noaa" | "synthetic"
MODE = os.getenv("MODE", "synthetic").lower()
DATA_FILE = os.getenv("DATA_FILE", "")          # noaa : plusieurs fichiers séparés par des virgules
LOCS_FILE = os.getenv("LOCS_FILE", "")          # intel : mote_locs.txt (optionnel)
ISD_HISTORY = os.getenv("ISD_HISTORY", "")      # noaa : isd-history.csv (optionnel, pour lat/lon)
RATE = float(os.getenv("REPLAY_RATE", "20"))    # messages par seconde
LOOP = os.getenv("REPLAY_LOOP", "false").lower() == "true"
MAX_ROWS = int(os.getenv("REPLAY_MAX_ROWS", "0"))  # 0 = tout le fichier

# (id, nom, latitude, longitude, température moyenne °C, amplitude jour/nuit °C)
STATIONS = [
    (1, "Agadir", 30.42, -9.60, 21.0, 4.0),
    (2, "Casablanca", 33.57, -7.59, 19.0, 5.0),
    (3, "Marrakech", 31.63, -8.01, 24.0, 9.0),
    (4, "Rabat", 34.02, -6.84, 18.5, 5.0),
    (5, "Tanger", 35.76, -5.83, 18.0, 5.0),
    (6, "Fes", 34.03, -5.00, 20.0, 8.0),
    (7, "Ouarzazate", 30.92, -6.89, 25.0, 11.0),
    (8, "Laayoune", 27.15, -13.20, 22.0, 5.0),
    (9, "Oujda", 34.68, -1.91, 20.0, 8.0),
    (10, "Essaouira", 31.51, -9.77, 19.0, 4.0),
]


class Station:
    def __init__(self, sid, name, lat, lon, t_mean, t_amp):
        self.sid, self.name, self.lat, self.lon = sid, name, lat, lon
        self.t_mean, self.t_amp = t_mean, t_amp
        self.drift = 0.0                         # variation lente de température
        self.pressure = 1013.0 + random.uniform(-5, 5)
        self.battery = random.uniform(2.9, 3.0)
        self.stuck_left = 0                      # nb d'envois restants avec valeurs figées
        self.last = None

    def measure(self, sim_time):
        hour = sim_time.hour + sim_time.minute / 60
        daily = self.t_amp * math.sin(2 * math.pi * (hour - 9) / 24)  # pic vers 15 h
        self.drift = 0.95 * self.drift + random.gauss(0, 0.15)
        temp = self.t_mean + daily + self.drift + random.gauss(0, 0.2)
        hum = 70 - 1.8 * (temp - self.t_mean) + random.gauss(0, 2)
        hum = min(100.0, max(5.0, hum))
        self.pressure += random.gauss(0, 0.2)
        self.pressure = min(1040.0, max(985.0, self.pressure))
        wind = abs(random.gauss(14, 7))
        self.battery = max(2.2, self.battery - 0.00005)
        return {
            "station_id": str(self.sid),
            "station_name": self.name,
            "latitude": self.lat,
            "longitude": self.lon,
            "ts": sim_time.isoformat(),
            "temperature": round(temp, 2),
            "humidity": round(hum, 2),
            "pressure": round(self.pressure, 2),
            "wind_speed": round(wind, 2),
            "battery_v": round(self.battery, 3),
        }

    def inject_fault(self, msg):
        """Simule un capteur défaillant. Retourne le type de panne ou None."""
        if self.stuck_left > 0 and self.last:
            self.stuck_left -= 1
            for k in ("temperature", "humidity", "pressure"):
                msg[k] = self.last[k]
            return "stuck"
        if random.random() >= FAULT_RATE:
            self.last = {k: msg[k] for k in ("temperature", "humidity", "pressure")}
            return None
        kind = random.choice(["spike", "out_of_range", "missing", "stuck"])
        if kind == "spike":
            msg["temperature"] = round(msg["temperature"] + random.choice([-1, 1]) * random.uniform(25, 50), 2)
        elif kind == "out_of_range":
            msg["humidity"] = random.choice([-999.0, 150.0])
        elif kind == "missing":
            msg["temperature"] = None
        else:
            self.stuck_left = random.randint(5, 15)
        return kind


def connect():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"simulateur-{os.getpid()}")
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            break
        except OSError as exc:
            print(f"Broker {MQTT_HOST}:{MQTT_PORT} indisponible ({exc}), nouvel essai dans 3 s...")
            time.sleep(3)
    client.loop_start()
    return client


STOP = False


def _stop(*_):
    global STOP
    STOP = True


def run_synthetic(client):
    stations = [Station(*s) for s in STATIONS[:NB_STATIONS]]
    print(f"Mode synthétique - {len(stations)} stations - topic {TOPIC_PREFIX}/<id>")
    sim_time = datetime.now(timezone.utc).replace(microsecond=0)
    while not STOP:
        for st in stations:
            msg = st.measure(sim_time)
            fault = st.inject_fault(msg)
            client.publish(f"{TOPIC_PREFIX}/{st.sid}", json.dumps(msg), qos=1)
            flag = f"  [PANNE simulée: {fault}]" if fault else ""
            print(f"{msg['ts']} station {st.sid} T={msg['temperature']} H={msg['humidity']}{flag}")
        sim_time += timedelta(minutes=SIM_STEP)
        time.sleep(INTERVAL)


def run_replay(client):
    files = [f for f in DATA_FILE.split(",") if f]
    missing = [f for f in files if not os.path.exists(f)]
    if not files or missing:
        print(f"ATTENTION : fichier(s) de données introuvable(s) {missing or '(DATA_FILE vide)'} "
              "-> bascule en mode synthétique.")
        return run_synthetic(client)

    def make():
        if MODE == "intel":
            return intel_records(files[0], LOCS_FILE)
        return noaa_records(files, ISD_HISTORY)

    print(f"Mode {MODE} - rejeu de {files} à {RATE:g} messages/s - topic {TOPIC_PREFIX}/<station_id>")
    delay = 1.0 / RATE
    while not STOP:
        n = 0
        for n, msg in enumerate(make(), 1):
            if STOP or (MAX_ROWS and n > MAX_ROWS):
                break
            msg["ingest_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            client.publish(f"{TOPIC_PREFIX}/{msg['station_id']}", json.dumps(msg), qos=1)
            if n % 500 == 0 or n <= 3:
                print(f"[{n}] station {msg['station_id']} ts={msg['ts']} T={msg['temperature']} H={msg['humidity']}")
            time.sleep(delay)
        print(f"Fin du fichier ({n} messages).")
        if not LOOP:
            break


def main():
    client = connect()
    print(f"Connecté à {MQTT_HOST}:{MQTT_PORT}")
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    if MODE in ("intel", "noaa"):
        run_replay(client)
    else:
        run_synthetic(client)
    client.loop_stop()
    client.disconnect()
    print("Simulateur arrêté.")


if __name__ == "__main__":
    main()
