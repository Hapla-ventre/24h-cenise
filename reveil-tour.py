#!/data/data/com.termux/files/usr/bin/python3
# reveil-tour.py -- position GPS en continu (un point toutes les ~5 s, le max que Termux:API
# sait fournir) + detection automatique des tours, envoyes a l'appli (Firestore), en silence.
# Ne fait rien tant qu'aucune "sortie" n'est active dans coureur.html.
#
# Les points sont envoyes par paquets toutes les 10 s dans des documents "morceaux de trace"
# (un par tranche de 5 min) : ca reste sous les quotas gratuits Firestore sur 24h et chaque
# document reste petit. Si le reseau tombe, rien n'est perdu : points et tours sont gardes
# en memoire et renvoyes des que ca repasse.

import json
import math
import queue
import subprocess
import threading
import time
import urllib.error
import urllib.request

PROJECT = "mes-randonnees-314e9"
DOCS = "projects/" + PROJECT + "/databases/(default)/documents"
BASE = "https://firestore.googleapis.com/v1/" + DOCS

LAT0 = 46.019485
LON0 = 6.449133
FAR_M = 100           # distance a depasser pour se "rearmer" apres un tour
ENTER_M = 30          # tour compte des qu'un point tombe a moins de 30 m du depart
CAPTURE_M = 80        # rattrapage : si aucun point n'est tombe dans les 30 m, on prend le point
MOVE_AWAY_M = 20      # le plus proche vu a <80 m, valide des qu'on s'en eloigne de 20 m
MIN_LAP_S = 480       # 8 min minimum entre deux tours detectes ici
LAP_DEDUP_S = 300     # un tour deja enregistre (manuel ou auto) a moins de 5 min = meme passage
MAX_ACCURACY_M = 50   # ignore les positions moins precises que ca
MAX_SPEED_MS = 7.0    # ~25 km/h : au-dela, un "saut" de position est un rebond GPS
UPLOAD_EVERY_S = 10
CHUNK_MS = 300000     # un document de trace par tranche de 5 min
SESSION_CHECK_S = 15
RESTART_STREAM_S = 25 # termux-location -r updates s'arrete seul apres 30 s : on relance avant


def haversine(lat1, lon1, lat2, lon2):
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def http(method, url, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


ERR = object()


def get_active_session():
    body = {"structuredQuery": {
        "from": [{"collectionId": "race_sessions"}],
        "where": {"fieldFilter": {"field": {"fieldPath": "active"}, "op": "EQUAL", "value": {"booleanValue": True}}},
        "limit": 1,
    }}
    try:
        results = http("POST", BASE + ":runQuery", body)
    except Exception:
        return ERR  # reseau : on garde la sortie connue
    for r in results:
        if "document" in r:
            return r["document"]["name"].split("/")[-1]
    return None


# ---- flux GPS ----
# Chaque "termux-location -r updates" vit 30 s ; on en lance un nouveau toutes les 25 s pour
# que deux flux se chevauchent et qu'il n'y ait pas de trou. Les doublons sont filtres ensuite.

def run_one_stream(out_q):
    try:
        p = subprocess.Popen(["termux-location", "-p", "gps", "-r", "updates"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    except Exception:
        return
    buf, depth = "", 0
    for ch in iter(lambda: p.stdout.read(1), ""):
        if ch == "{":
            depth += 1
        if depth > 0:
            buf += ch
        if ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    out_q.put((time.time(), json.loads(buf)))
                except ValueError:
                    pass
                buf = ""
    p.wait()


def location_streams(out_q):
    while True:
        threading.Thread(target=run_one_stream, args=(out_q,), daemon=True).start()
        time.sleep(RESTART_STREAM_S)


# ---- envois ----

def upload_points(session_id, pts):
    by_chunk = {}
    for p in pts:
        by_chunk.setdefault(p["t"] // CHUNK_MS, []).append(p)
    writes = []
    for k, chunk_pts in sorted(by_chunk.items()):
        writes.append({
            "update": {"name": DOCS + "/race_sessions/" + session_id + "_t" + str(k),
                       "fields": {"trackOf": {"stringValue": session_id}}},
            "updateMask": {"fieldPaths": ["trackOf"]},
            "updateTransforms": [{
                "fieldPath": "pts",
                "appendMissingElements": {"values": [{"mapValue": {"fields": {
                    "t": {"integerValue": str(p["t"])},
                    "lat": {"doubleValue": p["lat"]},
                    "lon": {"doubleValue": p["lon"]},
                    "a": {"integerValue": str(p["a"])},
                }}} for p in chunk_pts]},
            }],
        })
    http("POST", BASE + ":commit", {"writes": writes})


def send_lap(session_id, t_ms):
    """Ajoute le tour sauf si un tour (manuel ou auto) existe deja a moins de LAP_DEDUP_S.
    Renvoie True si c'est regle (envoye ou doublon), False s'il faut reessayer plus tard."""
    for _ in range(3):
        try:
            doc = http("GET", BASE + "/race_sessions/" + session_id + "?mask.fieldPaths=laps")
        except Exception:
            return False
        laps = doc.get("fields", {}).get("laps", {}).get("arrayValue", {}).get("values", [])
        for v in laps:
            t = int(v.get("mapValue", {}).get("fields", {}).get("t", {}).get("integerValue", "0"))
            if abs(t - t_ms) < LAP_DEDUP_S * 1000:
                print(time.strftime("%H:%M:%S"), "tour deja compte (manuel ?), rien a ajouter")
                return True
        body = {"writes": [{
            "transform": {
                "document": DOCS + "/race_sessions/" + session_id,
                "fieldTransforms": [
                    {"fieldPath": "laps", "appendMissingElements": {"values": [{"mapValue": {"fields": {
                        "t": {"integerValue": str(t_ms)},
                        "source": {"stringValue": "termux"},
                    }}}]}},
                    {"fieldPath": "updatedAt", "setToServerValue": "REQUEST_TIME"},
                ],
            },
            # n'ecrit que si personne n'a touche aux tours entre la lecture et l'ecriture
            "currentDocument": {"updateTime": doc["updateTime"]},
        }]}
        try:
            http("POST", BASE + ":commit", body)
            print(time.strftime("%H:%M:%S"), "tour envoye a", session_id)
            return True
        except urllib.error.HTTPError as e:
            if e.code in (400, 409):  # le document a bouge entre-temps : on relit et on recommence
                continue
            return False
        except Exception:
            return False
    return False


def main():
    try:
        subprocess.run(["termux-wake-lock"], timeout=10)
    except Exception:
        pass
    q = queue.Queue()
    threading.Thread(target=location_streams, args=(q,), daemon=True).start()
    print("reveil-tour: demarre (GPS continu), Ctrl+C pour arreter.")

    session_id = None
    next_session_check = 0.0
    pending_pts = []
    pending_laps = []
    last_upload = 0.0
    last_fix_t = 0.0
    last_good = None
    armed = False
    min_dist = None
    min_dist_t = None
    last_lap_t = 0.0
    last_status = 0.0
    last_lap_try = 0.0
    n_pts = 0

    while True:
        now = time.time()

        if now >= next_session_check:
            next_session_check = now + SESSION_CHECK_S
            sid = get_active_session()
            if sid is not ERR and sid != session_id:
                session_id = sid
                pending_pts, pending_laps = [], []
                armed, min_dist, min_dist_t, last_good, last_lap_t = False, None, None, None, 0.0
                print(time.strftime("%H:%M:%S"), "sortie active :", session_id or "aucune")

        try:
            recv_t, loc = q.get(timeout=1)
        except queue.Empty:
            loc = None

        if loc is not None and session_id and "latitude" in loc:
            fix_t = recv_t - loc.get("elapsedMs", 0) / 1000.0
            lat, lon, acc = loc["latitude"], loc["longitude"], loc.get("accuracy")
            ok = fix_t > last_fix_t + 0.5  # sinon : meme point recu par les deux flux
            if ok:
                last_fix_t = fix_t
            if ok and acc is not None and acc > MAX_ACCURACY_M:
                ok = False
            if ok and last_good is not None:
                plat, plon, pt = last_good
                if haversine(plat, plon, lat, lon) / max(fix_t - pt, 1.0) > MAX_SPEED_MS:
                    ok = False
            if ok:
                last_good = (lat, lon, fix_t)
                pending_pts.append({"t": int(fix_t * 1000), "lat": lat, "lon": lon, "a": int(round(acc or 0))})
                n_pts += 1

                d = haversine(lat, lon, LAT0, LON0)
                if not armed and d > FAR_M:
                    armed, min_dist, min_dist_t = True, None, None
                if armed:
                    if min_dist is None or d < min_dist:
                        min_dist, min_dist_t = d, fix_t
                    lap_t = None
                    if d <= ENTER_M:
                        lap_t = fix_t
                    elif min_dist <= CAPTURE_M and d > min_dist + MOVE_AWAY_M:
                        lap_t = min_dist_t
                    if lap_t is not None and lap_t - last_lap_t > MIN_LAP_S:
                        armed, min_dist, min_dist_t = False, None, None
                        last_lap_t = lap_t
                        pending_laps.append(int(lap_t * 1000))

        if session_id and pending_laps and now - last_lap_try >= 5:
            last_lap_try = now
            pending_laps = [t for t in pending_laps if not send_lap(session_id, t)]

        if session_id and pending_pts and now - last_upload >= UPLOAD_EVERY_S:
            last_upload = now
            batch = pending_pts[:2000]
            try:
                upload_points(session_id, batch)
                pending_pts = pending_pts[len(batch):]
            except Exception:
                print(time.strftime("%H:%M:%S"), "envoi position en attente (reseau ?):", len(pending_pts), "points")

        if session_id and now - last_status >= 300:
            last_status = now
            print(time.strftime("%H:%M:%S"), "ok :", n_pts, "points GPS depuis le lancement")


if __name__ == "__main__":
    main()
