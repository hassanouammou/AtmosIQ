"""Lecture des jeux de données réels (Intel Berkeley, NOAA ISD-Lite).
Chaque générateur produit des dictionnaires respectant le contrat de données :
station_id (str), station_name, latitude, longitude, ts (UTC ISO 8601),
temperature, humidity, pressure, wind_speed, battery_v, light
(les valeurs absentes de la source valent None)."""
import csv
import gzip
import heapq
import math
import os
from datetime import datetime, timezone


def _open(path):
    return gzip.open(path, "rt", errors="replace") if path.endswith(".gz") else open(path, errors="replace")


def _f(x):
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------- Intel Berkeley
def load_mote_locations(path, lat0, lon0):
    """mote_locs.txt : 'moteid x y' (en mètres dans le labo) -> lat/lon approximatives."""
    locs = {}
    if not path or not os.path.exists(path):
        return locs
    with open(path) as fh:
        for line in fh:
            p = line.split()
            if len(p) >= 3 and p[0].isdigit():
                x, y = _f(p[1]), _f(p[2])
                if x is None or y is None:
                    continue
                lat = lat0 + y / 111320.0
                lon = lon0 + x / (111320.0 * math.cos(math.radians(lat0)))
                locs[p[0]] = (round(lat, 6), round(lon, 6))
    return locs


def intel_records(path, locs_path=None, lat0=37.87, lon0=-122.26):
    """data.txt : date time epoch moteid temperature humidity light voltage"""
    locs = load_mote_locations(locs_path, lat0, lon0)
    sort_time = os.getenv("INTEL_SORT", "true").lower() == "true"

    def lines():
        # data.txt est rangé capteur par capteur : on le trie par date pour simuler un vrai flux
        with _open(path) as fh:
            if not sort_time:
                yield from fh
                return
            print("Tri chronologique du fichier en cours (30 à 60 s)...", flush=True)
            rows = []
            for line in fh:
                q = line.split(None, 2)
                if len(q) >= 3:
                    rows.append((q[0] + " " + q[1], line))
            rows.sort(key=lambda r: r[0])
            print(f"{len(rows)} lignes triées, début du rejeu.", flush=True)
            for _, line in rows:
                yield line

    if True:
        for line in lines():
            p = line.split()
            if len(p) < 4 or not p[3].isdigit():
                continue
            try:
                ts = datetime.fromisoformat(f"{p[0]} {p[1]}").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            p += [None] * (8 - len(p))          # lignes incomplètes : valeurs manquantes
            lat, lon = locs.get(p[3], (None, None))
            yield {
                "station_id": p[3],
                "station_name": f"mote-{p[3]}",
                "latitude": lat,
                "longitude": lon,
                "ts": ts.isoformat(),
                "temperature": _f(p[4]),
                "humidity": _f(p[5]),
                "light": _f(p[6]),
                "pressure": None,
                "wind_speed": None,
                "battery_v": _f(p[7]),
            }


# ----------------------------------------------------------------- NOAA ISD-Lite
def load_isd_history(path):
    """isd-history.csv -> {'USAF-WBAN': (nom, lat, lon)}"""
    meta = {}
    if not path or not os.path.exists(path):
        return meta
    with open(path, newline="", errors="replace") as fh:
        for r in csv.DictReader(fh):
            meta[f"{r['USAF']}-{r['WBAN']}"] = (r.get("STATION NAME", "").strip(), _f(r.get("LAT")), _f(r.get("LON")))
    return meta


def _rh(t, td):
    if t is None or td is None:
        return None
    a, b = 17.625, 243.04
    return round(max(0.0, min(100.0, 100 * math.exp(a * td / (b + td)) / math.exp(a * t / (b + t)))), 1)


def _isd_file(path, meta):
    """Un fichier ISD-Lite (une station, une année), nom : USAF-WBAN-ANNEE[.gz]"""
    base = os.path.basename(path).replace(".gz", "")
    usaf, wban, _year = base.split("-")[:3]
    sid = f"{usaf}-{wban}"
    name, lat, lon = meta.get(sid, (sid, None, None))
    with _open(path) as fh:
        for line in fh:
            p = line.split()
            if len(p) < 9:
                continue
            try:
                ts = datetime(int(p[0]), int(p[1]), int(p[2]), int(p[3]), tzinfo=timezone.utc)
            except ValueError:
                continue
            v = [None if x == "-9999" else _f(x) for x in p[4:9]]
            temp = v[0] / 10 if v[0] is not None else None
            dew = v[1] / 10 if v[1] is not None else None
            yield {
                "station_id": sid,
                "station_name": name,
                "latitude": lat,
                "longitude": lon,
                "ts": ts.isoformat(),
                "temperature": temp,
                "humidity": _rh(temp, dew),
                "pressure": v[2] / 10 if v[2] is not None else None,
                "wind_speed": round(v[4] / 10 * 3.6, 1) if v[4] is not None else None,  # m/s -> km/h
                "battery_v": None,
                "light": None,
            }


def noaa_records(paths, history_path=None):
    """Plusieurs stations : fusion par ordre chronologique."""
    meta = load_isd_history(history_path)
    return heapq.merge(*[_isd_file(p, meta) for p in paths], key=lambda m: m["ts"])
