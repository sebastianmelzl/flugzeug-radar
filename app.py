import sys
import os
import threading
import queue as _tts_queue_mod
import csv
import gzip
import json
import time

IS_MACOS = sys.platform == "darwin"

if not IS_MACOS:
    from gevent import monkey
    monkey.patch_all()

from flask import Flask, render_template, jsonify, request, Response
from flask_socketio import SocketIO
import requests
import math
import subprocess
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Berlin")

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
_async_mode = "threading" if IS_MACOS else "gevent"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode=_async_mode)

DEFAULT_LAT = 49.83577819633236
DEFAULT_LON = 8.831005333434314
SIGHTING_RADIUS_KM = 3.0
POLL_INTERVAL_S = 10
RAILWAY_URL = "https://flugzeug-radar-production.up.railway.app"

_data_dir = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_data_dir, "sightings.db")


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


# ── Aircraft database (OpenSky CSV) ──────────────────────────────────────────
_AC_DB = {}   # icao24_lower → year string or None
_AC_DB_READY = False
_AC_DB_LOCK  = threading.Lock()
_AC_DB_CACHE = os.path.join(_data_dir, "ac_db_cache.json.gz")
_AC_DB_URL   = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
_AC_DB_MAX_AGE_DAYS = 7


def _parse_year(val):
    v = val.strip().strip('"')
    if v and v not in ("\\N", "None") and len(v) >= 4 and v[:4].isdigit():
        return v[:4]
    return None


def _load_ac_database():
    global _AC_DB_READY
    # Try local gzip-JSON cache first; reject old format (values are plain strings/null)
    if os.path.exists(_AC_DB_CACHE):
        age = (time.time() - os.path.getmtime(_AC_DB_CACHE)) / 86400
        if age < _AC_DB_MAX_AGE_DAYS:
            try:
                with gzip.open(_AC_DB_CACHE, "rt", encoding="utf-8") as fh:
                    db = json.load(fh)
                # Detect old cache format: values were strings/null, new format is dicts
                sample = next((v for v in db.values() if v is not None), None)
                if sample is not None and not isinstance(sample, dict):
                    print("[AcDB] old cache format detected, rebuilding …", flush=True)
                    os.remove(_AC_DB_CACHE)
                else:
                    with _AC_DB_LOCK:
                        _AC_DB.update(db)
                        _AC_DB_READY = True
                    print(f"[AcDB] {len(db):,} records loaded from cache", flush=True)
                    return
            except Exception as e:
                print(f"[AcDB] cache read failed: {e}", flush=True)

    print("[AcDB] downloading OpenSky aircraft database …", flush=True)
    try:
        r = requests.get(_AC_DB_URL, timeout=180, stream=True)
        if r.status_code != 200:
            print(f"[AcDB] download failed: HTTP {r.status_code}", flush=True)
            return
        db = {}
        reader = csv.reader(r.iter_lines(decode_unicode=True))
        header = next(reader, None)
        if header is None:
            return
        def ci(name):
            try: return header.index(name)
            except ValueError: return -1
        i24   = ci("icao24")
        iblt  = ci("built")
        iff   = ci("firstFlightDate")
        ireg  = ci("registered")
        iser  = ci("serialnumber")
        imod  = ci("model")
        iop   = ci("operatorcallsign")
        for row in reader:
            if not row or i24 < 0 or i24 >= len(row):
                continue
            key = row[i24].strip().strip('"').lower()
            if not key:
                continue
            year = None
            for idx in (iblt, iff, ireg):
                if 0 <= idx < len(row):
                    year = _parse_year(row[idx])
                    if year:
                        break
            serial = row[iser].strip().strip('"') if 0 <= iser < len(row) else ""
            model  = row[imod].strip().strip('"') if 0 <= imod < len(row) else ""
            op     = row[iop].strip().strip('"')  if 0 <= iop  < len(row) else ""
            db[key] = {"y": year, "s": serial, "m": model, "o": op}
        with _AC_DB_LOCK:
            _AC_DB.update(db)
            _AC_DB_READY = True
        print(f"[AcDB] {len(db):,} records loaded from OpenSky", flush=True)
        try:
            with gzip.open(_AC_DB_CACHE, "wt", encoding="utf-8") as fh:
                json.dump(db, fh)
            print("[AcDB] cache saved", flush=True)
        except Exception as e:
            print(f"[AcDB] cache save failed: {e}", flush=True)
    except Exception as e:
        print(f"[AcDB] error: {e}", flush=True)


def _ac_db_get(icao24):
    """Return the _AC_DB entry dict for an icao24, or {}."""
    if not icao24:
        return {}
    with _AC_DB_LOCK:
        v = _AC_DB.get(icao24.lower().strip())
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    # Legacy string/None format: treat as year-only
    return {"y": v, "s": "", "m": "", "o": ""}


def ac_year(icao24):
    return _ac_db_get(icao24).get("y")


threading.Thread(target=_load_ac_database, daemon=True).start()
# ─────────────────────────────────────────────────────────────────────────────


# ── Per-aircraft metadata cache (SQLite + web fallbacks) ─────────────────────
_AC_META_DB      = os.path.join(_data_dir, "aircraft_meta.db")
_AC_META_MAX_AGE = 30 * 86400   # 30 days
_AC_META_QUEUED  = set()        # icao24s queued this session (avoids duplicates)
_AC_META_QUEUE   = []
_AC_META_Q_LOCK  = threading.Lock()


def _init_aircraft_meta_db():
    with sqlite3.connect(_AC_META_DB, timeout=10) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS aircraft_meta (
            icao24       TEXT PRIMARY KEY,
            registration TEXT DEFAULT '',
            type_code    TEXT DEFAULT '',
            type_name    TEXT DEFAULT '',
            serial_no    TEXT DEFAULT '',
            year         TEXT DEFAULT '',
            first_flight TEXT DEFAULT '',
            operator     TEXT DEFAULT '',
            source       TEXT DEFAULT '',
            fetched_at   REAL NOT NULL
        )""")
        for col_def in ("photo_url TEXT DEFAULT ''", "country TEXT DEFAULT ''"):
            try:
                conn.execute(f"ALTER TABLE aircraft_meta ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass


_init_aircraft_meta_db()


def _db_get_meta(icao24):
    try:
        with sqlite3.connect(_AC_META_DB, timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM aircraft_meta WHERE icao24 = ?", (icao24,)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def _db_save_meta(icao24, data):
    try:
        with sqlite3.connect(_AC_META_DB, timeout=5) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO aircraft_meta
                   (icao24, registration, type_code, type_name, serial_no,
                    year, first_flight, operator, source, photo_url, country, fetched_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (icao24,
                 data.get("registration", ""), data.get("type_code", ""),
                 data.get("type_name", ""),    data.get("serial_no", ""),
                 data.get("year", ""),         data.get("first_flight", ""),
                 data.get("operator", ""),     data.get("source", ""),
                 data.get("photo_url", ""),    data.get("country", ""),
                 time.time()))
    except Exception as e:
        print(f"[AcMeta] DB write: {e}", flush=True)


def _extract_year_str(val):
    s = str(val).strip()
    if len(s) >= 4 and s[:4].isdigit() and 1940 <= int(s[:4]) <= 2030:
        return s[:4]
    return ""


def _fetch_hexdb(icao24):
    """Basic aircraft info from hexdb.io (free, no auth)."""
    try:
        r = requests.get(
            f"https://hexdb.io/api/v1/aircraft/{icao24.upper()}",
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        )
        if r.status_code != 200 or not r.text.strip().startswith("{"):
            return None
        d = r.json()
        raw_year = d.get("YearBuilt") or d.get("Year") or d.get("Built") or ""
        return {
            "registration": d.get("Registration", ""),
            "type_code":    d.get("ICAOTypeCode", ""),
            "type_name":    d.get("Type", ""),
            "serial_no":    d.get("SerialNo", "") or d.get("Cn", ""),
            "year":         _extract_year_str(raw_year),
            "operator":     d.get("RegisteredOwners", "") or d.get("Operator", ""),
            "source":       "hexdb",
        }
    except Exception as e:
        print(f"[AcMeta] hexdb {icao24}: {e}", flush=True)
    return None


def _fetch_adsbdb(icao24):
    """Photo URL + basic info from adsbdb.com."""
    try:
        r = requests.get(
            f"https://api.adsbdb.com/v0/aircraft/{icao24.lower()}",
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code == 200:
            ac = r.json().get("response", {}).get("aircraft", {})
            if ac:
                return {
                    "registration": ac.get("registration", ""),
                    "type_code":    ac.get("icao_type", ""),
                    "type_name":    ac.get("type", ""),
                    "operator":     ac.get("registered_owner", ""),
                    "country":      ac.get("registered_owner_country_name", ""),
                    "photo_url":    ac.get("url_photo_thumbnail") or ac.get("url_photo") or "",
                    "source":       "adsbdb",
                }
    except Exception as e:
        print(f"[AcMeta] adsbdb {icao24}: {e}", flush=True)
    return None


def _fetch_planespotters(registration):
    """Year/MSN/photo from planespotters.net photo API (best-effort)."""
    if not registration:
        return None
    try:
        r = requests.get(
            f"https://api.planespotters.net/pub/photos/reg/{registration}",
            timeout=8,
            headers={"User-Agent": "FlugzeugRadar/1.0"}
        )
        if r.status_code != 200:
            return None
        photos = r.json().get("photos", [])
        if not photos:
            return None
        p = photos[0]
        plane = p.get("plane", {})
        year = _extract_year_str(
            plane.get("year_built") or plane.get("year") or
            plane.get("built") or ""
        )
        ff = str(plane.get("first_flight_date") or plane.get("first_flight") or "")
        if not year and ff:
            year = _extract_year_str(ff)
        photo_url = ((p.get("thumbnail_large") or {}).get("src") or
                     (p.get("thumbnail") or {}).get("src") or "")
        return {
            "serial_no":    str(plane.get("msn") or ""),
            "year":         year,
            "first_flight": ff,
            "photo_url":    photo_url,
            "source":       "planespotters",
        }
    except Exception as e:
        print(f"[AcMeta] planespotters {registration}: {e}", flush=True)
    return None


def _fetch_wikimedia_photo(registration):
    """Fetch a high-res aircraft photo from Wikimedia Commons (~900px wide)."""
    if not registration:
        return None
    try:
        reg = registration.upper().strip()
        r = requests.get(
            "https://commons.wikimedia.org/w/api.php",
            params={"action": "query", "list": "search", "srsearch": reg,
                    "srnamespace": "6", "srlimit": "5", "format": "json"},
            timeout=8,
            headers={"User-Agent": "FlugzeugRadar/1.0"}
        )
        if r.status_code != 200:
            return None
        results = r.json().get("query", {}).get("search", [])
        best_title = None
        for res in results:
            title = res.get("title", "")
            if reg in title.replace("File:", "").upper():
                best_title = title.replace("File:", "")
                break
        if not best_title:
            return None
        r2 = requests.get(
            "https://commons.wikimedia.org/w/api.php",
            params={"action": "query", "titles": f"File:{best_title}",
                    "prop": "imageinfo", "iiprop": "url|size", "iiurlwidth": "900",
                    "format": "json"},
            timeout=8,
            headers={"User-Agent": "FlugzeugRadar/1.0"}
        )
        if r2.status_code != 200:
            return None
        for pid, page in r2.json().get("query", {}).get("pages", {}).items():
            ii = page.get("imageinfo", [{}])[0]
            photo_url = ii.get("thumburl") or ii.get("url")
            if photo_url:
                return {"photo_url": photo_url}
    except Exception as e:
        print(f"[AcMeta] wikimedia {registration}: {e}", flush=True)
    return None


def _do_fetch_meta(icao24):
    result = {k: "" for k in (
        "registration", "type_code", "type_name", "serial_no",
        "year", "first_flight", "operator", "source", "photo_url", "country"
    )}

    # hexdb: registration, type, operator
    hd = _fetch_hexdb(icao24)
    if hd:
        result.update({k: v for k, v in hd.items() if v})

    # adsbdb: photo URL + country + fills any gaps
    ad = _fetch_adsbdb(icao24)
    if ad:
        for k in ("registration", "type_code", "type_name", "operator", "photo_url", "country"):
            if ad.get(k) and not result.get(k):
                result[k] = ad[k]

    # planespotters: year, MSN, photo fallback
    if result.get("registration") and (not result.get("year") or not result.get("serial_no") or not result.get("photo_url")):
        ps = _fetch_planespotters(result["registration"])
        if ps:
            for k in ("serial_no", "year", "first_flight"):
                if ps.get(k) and not result.get(k):
                    result[k] = ps[k]
            if ps.get("photo_url") and not result.get("photo_url"):
                result["photo_url"] = ps["photo_url"]

    # Wikimedia: high-res photo (~900px), overrides planespotters thumbnail
    if result.get("registration"):
        wm = _fetch_wikimedia_photo(result["registration"])
        if wm and wm.get("photo_url"):
            result["photo_url"] = wm["photo_url"]

    # OpenSky CSV year fallback
    if not result.get("year"):
        csv_yr = ac_year(icao24)
        if csv_yr:
            result["year"] = csv_yr
            if not result.get("source"):
                result["source"] = "opensky_csv"

    _db_save_meta(icao24, result)
    print(
        f"[AcMeta] {icao24} reg={result['registration']!r} year={result['year']!r} "
        f"msn={result['serial_no']!r} photo={'yes' if result.get('photo_url') else 'no'}",
        flush=True
    )
    return result


def _ac_meta_worker():
    while True:
        icao24 = None
        with _AC_META_Q_LOCK:
            if _AC_META_QUEUE:
                icao24 = _AC_META_QUEUE.pop(0)
        if icao24:
            try:
                cached = _db_get_meta(icao24)
                if not (cached and time.time() - cached.get("fetched_at", 0) < _AC_META_MAX_AGE):
                    _do_fetch_meta(icao24)
            except Exception as e:
                print(f"[AcMeta] worker: {e}", flush=True)
            time.sleep(0.5)  # gentle rate limit
        else:
            time.sleep(2)


threading.Thread(target=_ac_meta_worker, daemon=True).start()


def queue_ac_meta(icao24):
    if not icao24:
        return
    k = icao24.lower().strip()
    with _AC_META_Q_LOCK:
        if k not in _AC_META_QUEUED:
            _AC_META_QUEUED.add(k)
            _AC_META_QUEUE.append(k)


def enrich_flight(f):
    """Attach year/msn/type_name from SQLite meta and CSV cache; queue background fetch."""
    icao24  = (f.get("icao24") or "").lower().strip()
    meta    = _db_get_meta(icao24) if icao24 else None
    csv_row = _ac_db_get(icao24)
    f["year"]           = (meta or {}).get("year") or csv_row.get("y") or None
    f["msn"]            = (meta or {}).get("serial_no") or csv_row.get("s") or ""
    f["type_name_full"] = (meta or {}).get("type_name") or csv_row.get("m") or ""
    f["operator_full"]  = (meta or {}).get("operator") or csv_row.get("o") or ""
    queue_ac_meta(icao24)
# ─────────────────────────────────────────────────────────────────────────────

_last_payload = {"flights": [], "source": ""}


def _background_poller():
    logged_ids = set()
    while True:
        try:
            flights, source = fetch_flights(DEFAULT_LAT, DEFAULT_LON, radius_km=150)
            for f in flights:
                enrich_flight(f)
            payload = {"flights": flights, "source": source}
            _last_payload.update(payload)
            socketio.emit("flights_update", payload)
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


@socketio.on("connect")
def on_connect():
    if _last_payload["flights"]:
        socketio.emit("flights_update", _last_payload, to=request.sid)


@app.route("/")
def index():
    return render_template("index.html", default_lat=DEFAULT_LAT, default_lon=DEFAULT_LON, is_local=IS_MACOS)


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
        for f in flights:
            enrich_flight(f)
        flights.sort(key=lambda x: x["distance_km"])
        return jsonify({"flights": flights, "count": len(flights), "source": source})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/aircraft/<icao24_hex>")
def get_aircraft_info(icao24_hex):
    k = icao24_hex.lower().strip()
    meta = _db_get_meta(k) or {}
    csv_row = _ac_db_get(k)
    changed = False

    # Step 1: if basic data missing, fetch from hexdb + adsbdb inline
    if not meta.get("registration"):
        hd = _fetch_hexdb(k)
        if hd:
            for key in ("registration", "type_code", "type_name", "operator"):
                if hd.get(key) and not meta.get(key):
                    meta[key] = hd[key]
            meta.setdefault("source", "hexdb")
            changed = True
        ad = _fetch_adsbdb(k)
        if ad:
            for key in ("registration", "type_code", "type_name", "operator", "photo_url", "country"):
                if ad.get(key) and not meta.get(key):
                    meta[key] = ad[key]
            changed = True

    # Step 2: try Wikimedia for high-res photo (also upgrades cached planespotters thumbnails)
    if meta.get("registration"):
        current_photo = meta.get("photo_url", "")
        if not current_photo or "wikimedia" not in current_photo:
            try:
                wm = _fetch_wikimedia_photo(meta["registration"])
                if wm and wm.get("photo_url"):
                    meta["photo_url"] = wm["photo_url"]
                    changed = True
            except Exception:
                pass

    # Step 3: if still no photo, fall back to planespotters
    if not meta.get("photo_url") and meta.get("registration"):
        try:
            ps = _fetch_planespotters(meta["registration"])
            if ps:
                if ps.get("photo_url"):
                    meta["photo_url"] = ps["photo_url"]
                    changed = True
                for key in ("serial_no", "year", "first_flight"):
                    if ps.get(key) and not meta.get(key):
                        meta[key] = ps[key]
                        changed = True
        except Exception:
            pass

    # Persist any newly fetched data
    if changed and meta.get("registration"):
        meta.setdefault("fetched_at", time.time())
        _db_save_meta(k, meta)

    return jsonify({
        "icao24":       k,
        "registration": meta.get("registration") or "",
        "type_code":    meta.get("type_code") or "",
        "type_name":    meta.get("type_name") or csv_row.get("m", ""),
        "serial_no":    meta.get("serial_no") or csv_row.get("s", ""),
        "year":         meta.get("year") or csv_row.get("y") or "",
        "operator":     meta.get("operator") or csv_row.get("o", ""),
        "country":      meta.get("country") or "",
        "photo_url":    meta.get("photo_url") or "",
        "source":       meta.get("source") or "",
    })


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
_spotify_watchdog = None
_tts_queue = _tts_queue_mod.Queue()


def _tts_worker_loop():
    while True:
        item = _tts_queue.get()
        try:
            text, mp3_path, pause_spotify, chime_type, voice = item
            _speak_background(text, mp3_path, pause_spotify, chime_type, voice)
        except Exception as e:
            print(f"[TTS worker] error: {e}", flush=True)
        finally:
            _tts_queue.task_done()


threading.Thread(target=_tts_worker_loop, daemon=True).start()


def _generate_chime_wav():
    import wave, struct, math
    path = "/tmp/flugzeug_radar_chime.wav"
    rate = 44100

    # Simple sine-wave bell: one partial per note, gentle decay
    notes_hz = [523.25, 659.25, 783.99, 1046.50]  # C5 E5 G5 C6
    note_gap = 0.52   # seconds between note onsets
    decay_s  = 2.0    # how long each note rings
    decay_k  = 2.8    # exp(-decay_k) = amplitude at end of decay → ~6 %

    # Buffer covers from first onset to end of last note's ring + 0.5 s silence
    last_onset_s = (len(notes_hz) - 1) * note_gap
    total        = int(rate * (last_onset_s + decay_s + 0.5))
    mix          = [0.0] * total

    attack_n   = int(0.004 * rate)   # 4 ms attack
    k_step     = math.exp(-decay_k / (decay_s * rate))

    for ni, freq in enumerate(notes_hz):
        onset      = int(ni * note_gap * rate)
        n          = total - onset          # note plays to end of buffer
        phase_step = 2.0 * math.pi * freq / rate
        phase      = 0.0
        env        = 1.0
        for i in range(n):
            attack = i / attack_n if i < attack_n else 1.0
            mix[onset + i] += attack * env * math.sin(phase)
            env   *= k_step
            phase += phase_step

    # Normalise
    peak  = max(abs(v) for v in mix) or 1.0
    scale = 32767 * 0.80 / peak

    # 0.5 s fade-out at the very end – no abrupt cutoff
    fade_n = int(0.5 * rate)
    for i in range(fade_n):
        mix[total - fade_n + i] *= (fade_n - i) / fade_n

    data = b"".join(struct.pack("<h", max(-32767, min(32767, int(scale * v)))) for v in mix)
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(rate)
        wf.writeframes(data)
    return path


def _generate_simple_chime_wav():
    import wave, struct, math
    path = "/tmp/flugzeug_radar_chime_small.wav"
    rate = 44100
    freq = 880.0        # A5 — clean single ding
    decay_s = 1.2
    decay_k = 3.0
    total = int(rate * (decay_s + 0.3))
    mix = [0.0] * total
    attack_n = int(0.003 * rate)
    k_step = math.exp(-decay_k / (decay_s * rate))
    phase_step = 2.0 * math.pi * freq / rate
    phase = 0.0
    env = 1.0
    for i in range(total):
        attack = i / attack_n if i < attack_n else 1.0
        mix[i] = attack * env * math.sin(phase)
        env *= k_step
        phase += phase_step
    peak = max(abs(v) for v in mix) or 1.0
    scale = 32767 * 0.80 / peak
    fade_n = int(0.3 * rate)
    for i in range(fade_n):
        mix[total - fade_n + i] *= (fade_n - i) / fade_n
    data = b"".join(struct.pack("<h", max(-32767, min(32767, int(scale * v)))) for v in mix)
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(rate)
        wf.writeframes(data)
    return path


CHIME_BIG_PATH   = _generate_chime_wav()
CHIME_SMALL_PATH = _generate_simple_chime_wav()


def _osascript(script):
    try:
        return subprocess.run(["osascript", "-e", script],
                              capture_output=True, text=True, timeout=3).stdout.strip()
    except Exception:
        return ""


def _spotify_pause():
    global _spotify_watchdog
    # Check if Spotify is running WITHOUT launching it
    if _osascript('return (application "Spotify" is running) as string') != "true":
        return False
    state = _osascript('tell application "Spotify" to return player state as string')
    was_playing = (state == "playing")
    print(f"[Spotify] pause called: state='{state}' was_playing={was_playing}", flush=True)
    if was_playing:
        _osascript('tell application "Spotify" to pause')
        if _spotify_watchdog:
            _spotify_watchdog.cancel()
        _spotify_watchdog = threading.Timer(60, lambda: _osascript('tell application "Spotify" to play'))
        _spotify_watchdog.daemon = True
        _spotify_watchdog.start()
        print("[Spotify] watchdog armed (60s)", flush=True)
    return was_playing


ALLOWED_VOICES = {
    "de-DE-KatjaNeural", "de-DE-AmalaNeural", "de-DE-MajaNeural",
    "de-DE-SeraphinaNeural", "de-DE-ConradNeural", "de-DE-KillianNeural",
    "de-DE-RalfNeural", "de-AT-IngridNeural", "de-AT-JonasNeural", "de-CH-LeniNeural",
}
DEFAULT_VOICE = "de-CH-LeniNeural"

def _duck_volume():
    """Lower system volume to ~30 % of current.
    Returns (original_level, afplay_compensation) so TTS can play at full perceived loudness."""
    try:
        vol_str = _osascript('output volume of (get volume settings)')
        orig = int(vol_str) if vol_str.isdigit() else None
        if orig and orig > 15:
            ducked = max(8, round(orig * 0.30))
            _osascript(f'set volume output volume {ducked}')
            compensate = round(orig / ducked, 2)
            print(f"[Vol] ducked {orig} → {ducked}, afplay ×{compensate}", flush=True)
            return orig, compensate
        return orig, 1.0
    except Exception:
        return None, 1.0


def _restore_volume(orig):
    """Restore system output volume to orig (int 0-100)."""
    if orig is not None:
        try:
            _osascript(f'set volume output volume {orig}')
        except Exception:
            pass


def _speak_background(text, mp3_path, pause_spotify, chime_type=None, voice=DEFAULT_VOICE):
    global _speak_process, _spotify_watchdog
    print(f"[TTS] starting: '{text[:60]}' pause_spotify={pause_spotify} chime={chime_type} voice={voice}", flush=True)
    was_playing = False
    _orig_vol, _afplay_vol = _duck_volume()
    try:
        if pause_spotify:
            # Pause Spotify in a sub-thread so the chime starts immediately in parallel
            _pause_result = [False]
            def _do_pause():
                _pause_result[0] = _spotify_pause()
            _pt = threading.Thread(target=_do_pause, daemon=True)
            _pt.start()
        else:
            _pt = None

        # Generate TTS audio in parallel with chime so there's no gap between them
        tts_ok = [False]
        def _gen_tts():
            try:
                subprocess.run(
                    ["python3", "-m", "edge_tts", "--voice", voice,
                     "--text", text, "--write-media", mp3_path],
                    check=True, timeout=12
                )
                tts_ok[0] = True
                print("[TTS] edge_tts ready", flush=True)
            except Exception as e:
                print(f"[TTS] edge_tts failed: {e}", flush=True)

        tts_thread = threading.Thread(target=_gen_tts, daemon=True)
        tts_thread.start()

        if chime_type:
            chime_path = CHIME_BIG_PATH if chime_type == "big" else CHIME_SMALL_PATH
            print(f"[TTS] playing chime ({chime_type})", flush=True)
            try:
                cp = subprocess.Popen(["afplay", "-v", str(_afplay_vol), chime_path])
                cp.wait(timeout=7)
            except Exception as e:
                print(f"[TTS] chime failed: {e}", flush=True)

        if _pt:
            _pt.join(timeout=5)
            was_playing = _pause_result[0]

        tts_thread.join(timeout=15)  # should already be done; wait at most 15 s
        if tts_ok[0]:
            print("[TTS] playing mp3", flush=True)
            _speak_process = subprocess.Popen(["afplay", "-v", str(_afplay_vol), mp3_path])
            # Schedule Spotify resume 200 ms before TTS ends so it kicks in seamlessly
            if was_playing:
                try:
                    info = subprocess.run(["afinfo", mp3_path],
                                          capture_output=True, text=True, timeout=2).stdout
                    import re as _re
                    m = _re.search(r'estimated duration:\s*([\d.]+)', info)
                    dur = float(m.group(1)) if m else None
                except Exception:
                    dur = None
                if dur and dur > 0.3:
                    lead = 0.20
                    def _resume_early():
                        print("[Spotify] early resume", flush=True)
                        subprocess.Popen(["osascript", "-e",
                                          'tell application "Spotify" to play'])
                    rt = threading.Timer(max(0.0, dur - lead), _resume_early)
                    rt.daemon = True
                    rt.start()
                    was_playing = False   # handled; skip fallback in finally
        else:
            print("[TTS] falling back to say", flush=True)
            _speak_process = subprocess.Popen(["say", "-v", "Anna", "-r", "160", text])
        if _speak_process:
            _speak_process.wait(timeout=30)
        print("[TTS] playback done", flush=True)
    except Exception as e:
        print(f"[TTS] error during playback: {e}", flush=True)
    finally:
        if _spotify_watchdog:
            _spotify_watchdog.cancel()
            _spotify_watchdog = None
        if was_playing:
            print("[Spotify] resuming after TTS", flush=True)
            _osascript('tell application "Spotify" to play')
        _restore_volume(_orig_vol)
        print(f"[Vol] restored to {_orig_vol}", flush=True)



@app.route("/api/spotify/pause", methods=["POST"])
def spotify_pause():
    if not IS_MACOS:
        return jsonify({"ok": False}), 200
    was_playing = _spotify_pause()
    return jsonify({"ok": True, "was_playing": was_playing})


@app.route("/api/spotify/resume", methods=["POST"])
def spotify_resume():
    if not IS_MACOS:
        return jsonify({"ok": False}), 200
    print("[Spotify] manual resume called", flush=True)
    _osascript('tell application "Spotify" to play')
    return jsonify({"ok": True})


@app.route("/api/speak", methods=["POST"])
def speak():
    if not IS_MACOS:
        return jsonify({"ok": False, "reason": "TTS nur lokal verfügbar"}), 200
    text = request.json.get("text", "").strip()
    if not text:
        return jsonify({"error": "no text"}), 400
    mp3_path = f"/tmp/flugzeug_radar_speech_{int(time.time() * 1000)}.mp3"
    pause_spotify = bool(request.json.get("pause_spotify", False))
    chime_type    = request.json.get("chime_type") or None
    if chime_type not in ("big", "small", None):
        chime_type = None
    voice = request.json.get("voice", DEFAULT_VOICE)
    if voice not in ALLOWED_VOICES:
        voice = DEFAULT_VOICE
    _tts_queue.put((text, mp3_path, pause_spotify, chime_type, voice))
    return jsonify({"ok": True})


@app.route("/api/tts", methods=["POST"])
def api_tts():
    """Generate TTS audio and return MP3 — works on all platforms."""
    data = request.json or {}
    text = data.get("text", "").strip()
    if not text:
        return jsonify({"error": "no text"}), 400
    voice = data.get("voice", DEFAULT_VOICE)
    if voice not in ALLOWED_VOICES:
        voice = DEFAULT_VOICE
    mp3_path = f"/tmp/flugzeug_tts_{int(time.time() * 1000)}_{os.getpid()}.mp3"
    try:
        subprocess.run(
            ["python3", "-m", "edge_tts", "--voice", voice, "--text", text, "--write-media", mp3_path],
            check=True, timeout=15, capture_output=True
        )
        with open(mp3_path, "rb") as f:
            audio_data = f.read()
        return Response(audio_data, mimetype="audio/mpeg",
                        headers={"Cache-Control": "no-cache", "Content-Length": str(len(audio_data))})
    except Exception as e:
        print(f"[TTS] /api/tts error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            os.remove(mp3_path)
        except OSError:
            pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5004))
    socketio.run(app, debug=False, port=port, allow_unsafe_werkzeug=True)
