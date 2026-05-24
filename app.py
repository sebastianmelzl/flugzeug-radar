import eventlet
eventlet.monkey_patch()

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
import requests
import math
import subprocess
import sqlite3
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

DEFAULT_LAT = 49.83580017089844
DEFAULT_LON = 8.829106330871582
SIGHTING_RADIUS_KM = 3.0
POLL_INTERVAL_S = 10
RAILWAY_URL = "https://flugzeug-radar-production.up.railway.app"

_data_dir = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_data_dir, "sightings.db")

IS_MACOS = sys.platform == "darwin"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS sightings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            hour INTEGER NOT NULL,
            weekday INTEGER NOT NULL,
            date TEXT NOT NULL,
            flight_id TEXT,
            callsign TEXT,
            airline_iata TEXT,
            airline_icao TEXT,
            aircraft_code TEXT,
            origin_airport_iata TEXT,
            destination_airport_iata TEXT,
            distance_km REAL,
            altitude_ft INTEGER,
            ground_speed_kmh INTEGER
        )""")


init_db()


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def fetch_flights(lat, lon, radius_km):
    flights = []
    try:
        from FlightRadar24 import FlightRadar24API
        fr_api = FlightRadar24API()
        bounds = fr_api.get_bounds_by_point(lat, lon, radius_km * 1000)
        raw_flights = fr_api.get_flights(bounds=bounds)

        for f in raw_flights:
            if f.on_ground:
                continue
            distance = haversine(lat, lon, f.latitude, f.longitude)
            alt_ft = f.altitude or 0
            spd_kt = f.ground_speed or 0
            vs = getattr(f, "vertical_speed", 0) or 0
            flights.append({
                "id": str(f.id),
                "callsign": (f.callsign or "").strip(),
                "flight_number": getattr(f, "number", "") or "",
                "registration": f.registration or "",
                "airline_iata": getattr(f, "airline_iata", "") or "",
                "airline_icao": getattr(f, "airline_icao", "") or "",
                "aircraft_code": getattr(f, "aircraft_code", "") or "",
                "origin_airport_iata": getattr(f, "origin_airport_iata", "") or "",
                "destination_airport_iata": getattr(f, "destination_airport_iata", "") or "",
                "latitude": f.latitude,
                "longitude": f.longitude,
                "altitude_ft": alt_ft,
                "altitude_m": round(alt_ft * 0.3048),
                "ground_speed_kt": spd_kt,
                "ground_speed_kmh": round(spd_kt * 1.852),
                "heading": f.heading or 0,
                "vertical_speed_fpm": round(vs),
                "squawk": getattr(f, "squawk", "") or "",
                "icao24": getattr(f, "icao_24bit", "") or "",
                "distance_km": round(distance, 1),
                "source": "fr24",
            })
        return flights, "fr24"

    except Exception:
        pass

    try:
        lat_d = radius_km / 111.0
        lon_d = radius_km / (111.0 * math.cos(math.radians(lat)))
        resp = requests.get(
            "https://opensky-network.org/api/states/all",
            params={"lamin": lat - lat_d, "lamax": lat + lat_d,
                    "lomin": lon - lon_d, "lomax": lon + lon_d},
            timeout=15,
        )
        data = resp.json()
        for state in data.get("states") or []:
            if state[5] is None or state[6] is None or state[8]:
                continue
            flight_lat, flight_lon = state[6], state[5]
            distance = haversine(lat, lon, flight_lat, flight_lon)
            alt_m = state[7] or state[13] or 0
            spd_ms = state[9] or 0
            vs_ms = state[11] or 0
            flights.append({
                "id": state[0],
                "callsign": (state[1] or "").strip(),
                "flight_number": "", "registration": "",
                "airline_iata": "", "airline_icao": "", "aircraft_code": "",
                "origin_airport_iata": "", "destination_airport_iata": "",
                "latitude": flight_lat, "longitude": flight_lon,
                "altitude_ft": round(alt_m * 3.28084),
                "altitude_m": round(alt_m),
                "ground_speed_kt": round(spd_ms * 1.94384),
                "ground_speed_kmh": round(spd_ms * 3.6),
                "heading": state[10] or 0,
                "vertical_speed_fpm": round(vs_ms * 196.85),
                "squawk": state[14] or "",
                "icao24": state[0],
                "distance_km": round(distance, 1),
                "source": "opensky",
            })
        return flights, "opensky"
    except Exception:
        return [], "error"


def save_sighting(f):
    now = datetime.now(tz=TZ)
    flight_id = f.get("id", "")
    with sqlite3.connect(DB_PATH) as conn:
        if flight_id and conn.execute(
            "SELECT 1 FROM sightings WHERE flight_id = ? AND date = ?",
            (flight_id, now.strftime("%Y-%m-%d"))
        ).fetchone():
            return
        conn.execute(
            """INSERT INTO sightings
               (timestamp, hour, weekday, date, flight_id, callsign,
                airline_iata, airline_icao, aircraft_code,
                origin_airport_iata, destination_airport_iata,
                distance_km, altitude_ft, ground_speed_kmh)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now.isoformat(), now.hour, now.weekday(), now.strftime("%Y-%m-%d"),
             f.get("id", ""), f.get("callsign", ""),
             f.get("airline_iata", ""), f.get("airline_icao", ""),
             f.get("aircraft_code", ""),
             f.get("origin_airport_iata", ""), f.get("destination_airport_iata", ""),
             f.get("distance_km", 0), f.get("altitude_ft", 0), f.get("ground_speed_kmh", 0)),
        )


def _background_poller():
    logged_ids = set()
    while True:
        try:
            flights, source = fetch_flights(DEFAULT_LAT, DEFAULT_LON, radius_km=150)
            socketio.emit("flights_update", {"flights": flights, "source": source})
            current_ids = {f["id"] for f in flights}
            for f in flights:
                if (f["distance_km"] <= SIGHTING_RADIUS_KM
                        and f["id"] not in logged_ids
                        and (f.get("airline_iata") or f.get("airline_icao"))
                        and f.get("origin_airport_iata")
                        and f.get("destination_airport_iata")):
                    logged_ids.add(f["id"])
                    save_sighting(f)
            logged_ids &= current_ids
        except Exception:
            pass
        socketio.sleep(POLL_INTERVAL_S)


socketio.start_background_task(_background_poller)


@app.route("/")
def index():
    return render_template("index.html", default_lat=DEFAULT_LAT, default_lon=DEFAULT_LON)


@app.route("/stats")
def stats_page():
    return render_template("stats.html")


@app.route("/api/flights")
def get_flights():
    try:
        lat = float(request.args.get("lat", DEFAULT_LAT))
        lon = float(request.args.get("lon", DEFAULT_LON))
        radius_km = float(request.args.get("radius", 50))
        flights, source = fetch_flights(lat, lon, radius_km)
        flights.sort(key=lambda x: x["distance_km"])
        return jsonify({"flights": flights, "count": len(flights), "source": source})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sighting/<int:sighting_id>", methods=["DELETE"])
def delete_sighting(sighting_id):
    if IS_MACOS:
        try:
            resp = requests.delete(f"{RAILWAY_URL}/api/sighting/{sighting_id}", timeout=10)
            return jsonify(resp.json())
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM sightings WHERE id = ?", (sighting_id,))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats")
def get_stats():
    if IS_MACOS:
        try:
            resp = requests.get(f"{RAILWAY_URL}/api/stats", timeout=10)
            return jsonify(resp.json())
        except Exception:
            pass
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            total = conn.execute("SELECT COUNT(*) as n FROM sightings").fetchone()["n"]
            hourly_raw = {r["hour"]: r["count"] for r in conn.execute(
                "SELECT hour, COUNT(*) as count FROM sightings GROUP BY hour"
            ).fetchall()}
            hourly = [{"hour": h, "count": hourly_raw.get(h, 0)} for h in range(24)]
            weekday_raw = {r["weekday"]: r["count"] for r in conn.execute(
                "SELECT weekday, COUNT(*) as count FROM sightings GROUP BY weekday"
            ).fetchall()}
            weekdays = [{"day": d, "count": weekday_raw.get(d, 0)} for d in range(7)]
            aircraft = [dict(r) for r in conn.execute(
                """SELECT aircraft_code, COUNT(*) as count FROM sightings
                   WHERE aircraft_code != '' GROUP BY aircraft_code
                   ORDER BY count DESC LIMIT 15"""
            ).fetchall()]
            airlines = [dict(r) for r in conn.execute(
                """SELECT airline_iata, airline_icao, COUNT(*) as count FROM sightings
                   WHERE airline_iata != '' OR airline_icao != ''
                   GROUP BY airline_iata, airline_icao ORDER BY count DESC LIMIT 15"""
            ).fetchall()]
            routes = [dict(r) for r in conn.execute(
                """SELECT origin_airport_iata, destination_airport_iata, COUNT(*) as count
                   FROM sightings WHERE origin_airport_iata != '' AND destination_airport_iata != ''
                   GROUP BY origin_airport_iata, destination_airport_iata
                   ORDER BY count DESC LIMIT 15"""
            ).fetchall()]
            recent = [dict(r) for r in conn.execute(
                "SELECT * FROM sightings ORDER BY timestamp DESC LIMIT 100"
            ).fetchall()]
            daily = [dict(r) for r in conn.execute(
                """SELECT date, COUNT(*) as count FROM sightings
                   GROUP BY date ORDER BY date DESC LIMIT 30"""
            ).fetchall()]
            heatmap_raw = conn.execute(
                "SELECT weekday, hour, COUNT(*) as count FROM sightings GROUP BY weekday, hour"
            ).fetchall()
            heatmap = {f"{r['weekday']},{r['hour']}": r['count'] for r in heatmap_raw}
            return jsonify({"total": total, "hourly": hourly, "weekdays": weekdays,
                            "aircraft": aircraft, "airlines": airlines, "routes": routes,
                            "recent": recent, "daily": daily, "heatmap": heatmap})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


_speak_process = None


@app.route("/api/speak", methods=["POST"])
def speak():
    global _speak_process
    if not IS_MACOS:
        return jsonify({"ok": False, "reason": "TTS nur lokal verfügbar"}), 200
    text = request.json.get("text", "").strip()
    if not text:
        return jsonify({"error": "no text"}), 400
    if _speak_process and _speak_process.poll() is None:
        _speak_process.terminate()
    mp3_path = "/tmp/flugzeug_radar_speech.mp3"
    try:
        subprocess.run(
            ["python3", "-m", "edge_tts", "--voice", "de-DE-KatjaNeural",
             "--text", text, "--write-media", mp3_path],
            check=True
        )
        _speak_process = subprocess.Popen(["afplay", mp3_path])
        return jsonify({"ok": True})
    except Exception as e:
        _speak_process = subprocess.Popen(["say", "-v", "Anna", "-r", "160", text])
        return jsonify({"ok": True, "fallback": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    socketio.run(app, debug=IS_MACOS, port=port)
