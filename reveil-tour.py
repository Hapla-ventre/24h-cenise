#!/data/data/com.termux/files/usr/bin/python3
# reveil-tour.py -- le coeur de l'appli coureur sur le telephone, qui marche SANS reseau :
#  - sert l'appli coureur en local : http://localhost:8765/coureur.html (s'ouvre sans internet)
#  - lit la position GPS en continu (un point toutes les ~5 s, le max que Termux:API sait fournir)
#  - detecte les tours au passage du depart
#  - donne positions et tours a la page coureur tout de suite, en local, sans passer par internet
#  - envoie tout vers Firestore (le suivi) des qu'il y a du reseau ; sans reseau, tout est garde
#    sur le telephone (meme si Termux redemarre) et renvoye plus tard, rien n'est perdu.
# Ne fait rien tant qu'aucune "sortie" n'est active dans coureur.html.

import json
import math
import os
import queue
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

PROJECT = "mes-randonnees-314e9"
DOCS = "projects/" + PROJECT + "/databases/(default)/documents"
BASE = "https://firestore.googleapis.com/v1/" + DOCS

HERE = os.path.dirname(os.path.abspath(__file__))
# hors du dossier de l'appli : une mise a jour (qui remplace ce dossier) n'efface pas la course
DATA_DIR = os.path.expanduser("~/.reveil-tour")
STATE_FILE = os.path.join(DATA_DIR, "etat.json")  # sortie suivie ; l'etat de chaque sortie est dans etat-<id>.json
PORT = 8765

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
CHUNK_MS = 900000     # un document de trace par tranche de 15 min (moins de lectures pour chaque suiveur)
SESSION_CHECK_S = 15
PAGE_TRUST_S = 600    # la page coureur ouverte fait foi sur la sortie en cours pendant 10 min
STREAM_MAX_S = 45     # filet de securite seulement : Termux:API termine lui-meme une demande en 30 s.
                      # On ne coupe JAMAIS une demande avant : des demandes coupees continuent de
                      # tourner 30 s dans Termux:API, s'accumulent et finissent par le bloquer.
FRESH_MS = 3000       # une position mesuree il y a moins de 3 s (sinon : la derniere en memoire)


def haversine(lat1, lon1, lat2, lon2):
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


# ---- etat partage (boucle principale + serveur local) ----

class Live:
    def __init__(self):
        self.lock = threading.Lock()
        self.session_id = None
        self.points = []         # points acceptes de la sortie en cours, dans l'ordre
        self.laps = []           # tours detectes ici (ms)
        self.pending_laps = []   # tours pas encore confirmes par Firestore
        self.uploaded_until = 0  # dernier point confirme par Firestore (ms)
        self.last_upload = 0     # heure du dernier envoi reussi (ms)
        self.started = None      # heure du depart de la course (ms), None tant qu'elle n'a pas demarre
        self.page_session = None
        self.page_started = None
        self.page_heard = 0.0
        self.remote = None       # (sortie, depart) vus dans Firestore
        self.online = False

    def points_file(self, sid):
        return os.path.join(DATA_DIR, "points-" + sid + ".jsonl")

    def session_file(self, sid):
        return os.path.join(DATA_DIR, "etat-" + sid + ".json")

    def save(self):
        """Enregistre l'etat de la sortie suivie. Appele avec le verrou tenu."""
        def write(path, data):
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, path)
        write(STATE_FILE, {"session_id": self.session_id})
        if self.session_id:
            write(self.session_file(self.session_id), {
                "started": self.started, "laps": self.laps,
                "pending_laps": self.pending_laps, "uploaded_until": self.uploaded_until})

    def load_points(self, sid):
        pts = []
        try:
            with open(self.points_file(sid)) as f:
                for line in f:
                    try:
                        pts.append(json.loads(line))
                    except ValueError:
                        pass  # derniere ligne coupee par un arret brutal
        except OSError:
            pass
        return pts

    def load_session(self, sid):
        """Charge tout ce qui est garde pour une sortie (points, tours, envois). Verrou tenu."""
        self.session_id = sid
        self.started, self.laps, self.pending_laps, self.uploaded_until = None, [], [], 0
        self.points = []
        if not sid:
            return
        self.points = self.load_points(sid)
        try:
            with open(self.session_file(sid)) as f:
                st = json.load(f)
            self.started = st.get("started")
            self.laps = st.get("laps", [])
            self.pending_laps = st.get("pending_laps", [])
            self.uploaded_until = st.get("uploaded_until", 0)
        except (OSError, ValueError):
            pass

    def restore(self):
        try:
            with open(STATE_FILE) as f:
                self.load_session(json.load(f).get("session_id"))
        except (OSError, ValueError):
            pass

    def switch(self, sid):
        """Change de sortie sans rien perdre de l'ancienne (tout est deja sur disque). Verrou tenu."""
        self.save()
        self.load_session(sid)
        self.save()

    def add_point(self, p):
        with self.lock:
            self.points.append(p)
            with open(self.points_file(self.session_id), "a") as f:
                f.write(json.dumps(p) + "\n")


live = Live()


# ---- serveur local : l'appli coureur + ses donnees en direct ----

class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HERE, **kwargs)

    def log_message(self, *args):
        pass

    def end_headers(self):
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path == "/":
            self.send_response(302)
            self.send_header("Location", "/coureur.html")
            self.end_headers()
            return
        if url.path != "/api/live":
            return super().do_GET()
        q = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        since = int(q.get("since", ["0"])[0] or 0)
        with live.lock:
            if "session" in q:
                live.page_session = (q.get("session", [""])[0] or None)
                live.page_started = int(q.get("started", ["0"])[0] or 0) or None
                live.page_heard = time.time()
            same = live.page_session is not None and live.page_session == live.session_id
            body = {
                "session": live.session_id,
                "points": [p for p in live.points if p["t"] > since] if same else [],
                "laps": list(live.laps) if same else [],
                "online": live.online,
                "pendingPts": sum(1 for p in live.points if p["t"] > live.uploaded_until),
                "pendingLaps": len(live.pending_laps),
                "lastUpload": live.last_upload,
            }
        data = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def serve():
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    except OSError as e:
        log("serveur local impossible (deja lance dans un autre onglet Termux ?) :", e)
        return
    srv.daemon_threads = True
    srv.serve_forever()


# ---- Firestore ----

def http(method, url, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            out = json.loads(resp.read().decode("utf-8"))
        live.online = True
        return out
    except urllib.error.HTTPError:
        live.online = True  # le serveur a repondu : le reseau marche
        raise
    except Exception:
        live.online = False
        raise


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
        return ERR
    for r in results:
        if "document" in r:
            started = r["document"].get("fields", {}).get("startedAt", {}).get("integerValue")
            return (r["document"]["name"].split("/")[-1], int(started) if started else None)
    return (None, None)


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
    Renvoie True si c'est regle (envoye ou doublon), False s'il faut reessayer plus tard
    (pas de reseau, ou sortie creee hors ligne pas encore arrivee dans Firestore)."""
    for _ in range(3):
        try:
            doc = http("GET", BASE + "/race_sessions/" + session_id + "?mask.fieldPaths=laps")
        except Exception:
            return False
        laps = doc.get("fields", {}).get("laps", {}).get("arrayValue", {}).get("values", [])
        for v in laps:
            t = int(v.get("mapValue", {}).get("fields", {}).get("t", {}).get("integerValue", "0"))
            if abs(t - t_ms) < LAP_DEDUP_S * 1000:
                log("tour deja compte dans Firestore, rien a ajouter")
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
            log("tour envoye au suivi")
            return True
        except urllib.error.HTTPError as e:
            if e.code in (400, 409):  # le document a bouge entre-temps : on relit et on recommence
                continue
            return False
        except Exception:
            return False
    return False


# ---- flux GPS ----
# Deux facons de demander la position a Termux:API, une seule demande a la fois, jamais coupee :
#  - "updates" : flux continu pendant 30 s (une position toutes les 5 s si Android le permet) ;
#  - "once"    : attend UNE position fraiche, la renvoie et se termine aussitot.
# En arriere-plan, Android ne donne souvent qu'une position fraiche par demande : le flux continu
# ne rapporte alors qu'un point toutes les ~30 s. Le script l'apprend et passe en "once" enchaines
# (un point des qu'il est calcule, ~5-10 s dehors) ; une demande sur 10 reessaie le flux continu.
stream_mode = {"once": False, "count": 0}


def run_one_request(out_q, kind):
    """Lance une demande et renvoie le nombre de positions fraiches recues."""
    try:
        p = subprocess.Popen(["termux-location", "-p", "gps", "-r", kind],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    except Exception:
        return 0
    killer = threading.Timer(STREAM_MAX_S, p.kill)  # demande vraiment bloquee seulement
    killer.daemon = True
    killer.start()
    fresh, buf, depth = 0, "", 0
    for ch in iter(lambda: p.stdout.read(1), ""):
        if ch == "{":
            depth += 1
        if depth > 0:
            buf += ch
        if ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    loc = json.loads(buf)
                    out_q.put((time.time(), loc))
                    if "latitude" in loc and loc.get("elapsedMs", 0) < FRESH_MS:
                        fresh += 1
                except ValueError:
                    pass
                buf = ""
    p.wait()
    killer.cancel()
    return fresh


def location_streams(out_q):
    while True:
        if not live.session_id:
            time.sleep(3)              # pas de sortie active : GPS eteint
            continue
        stream_mode["count"] += 1
        retry_updates = stream_mode["count"] % 10 == 0
        kind = "once" if stream_mode["once"] and not retry_updates else "updates"
        t0 = time.time()
        fresh = run_one_request(out_q, kind)
        if kind == "updates":
            # flux continu qui ne donne qu'une position fraiche (ou aucune) : Android bride
            stream_mode["once"] = fresh <= 1
        if time.time() - t0 < 3:
            time.sleep(3)              # erreur immediate (Termux:API absent...) : pas de boucle folle


# ---- envois vers Firestore, dans leur propre fil ----
# Le reseau peut etre lent ou absent (15 s d'attente par envoi) : il ne doit jamais retarder la
# lecture du GPS ni la detection des tours, qui se font dans la boucle principale.

def net_loop():
    next_session_check = 0.0
    last_upload = 0.0
    last_lap_try = 0.0
    was_online = None
    while True:
        now = time.time()
        with live.lock:
            page_fresh = now - live.page_heard < PAGE_TRUST_S
        if not page_fresh and now >= next_session_check:
            next_session_check = now + SESSION_CHECK_S
            r = get_active_session()
            if r is not ERR:
                with live.lock:
                    live.remote = r
        session_id = live.session_id

        if session_id and live.pending_laps and now - last_lap_try >= 10:
            last_lap_try = now
            done = [t for t in list(live.pending_laps) if send_lap(session_id, t)]
            if done:
                with live.lock:
                    if live.session_id == session_id:
                        live.pending_laps = [t for t in live.pending_laps if t not in done]
                        live.save()

        if session_id and now - last_upload >= UPLOAD_EVERY_S:
            last_upload = now
            with live.lock:
                batch = [p for p in live.points if p["t"] > live.uploaded_until][:2000]
            if batch:
                try:
                    upload_points(session_id, batch)
                    with live.lock:
                        live.last_upload = int(time.time() * 1000)
                        if live.session_id == session_id:
                            live.uploaded_until = max(live.uploaded_until, batch[-1]["t"])
                            live.save()
                except Exception:
                    pass

        if was_online is not None and live.online != was_online:
            log("reseau revenu, envoi de ce qui attendait" if live.online else "plus de reseau : tout est garde sur le telephone")
        was_online = live.online
        time.sleep(2)


# ---- boucle principale : GPS + tours ----

def already_running():
    s = socket.socket()
    s.settimeout(1)
    try:
        return s.connect_ex(("127.0.0.1", PORT)) == 0
    finally:
        s.close()


def wake_lock(on):
    # garde le telephone eveille pendant une sortie seulement
    try:
        subprocess.run(["termux-wake-lock" if on else "termux-wake-unlock"], timeout=10)
    except Exception:
        pass


def main(stream=location_streams):
    # lance automatiquement a chaque ouverture de Termux : un seul exemplaire a la fois
    if already_running():
        print("reveil-tour tourne deja (dans un autre onglet Termux). Appli : http://localhost:%d/coureur.html" % PORT)
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    live.restore()
    wake_lock(bool(live.session_id))
    threading.Thread(target=serve, daemon=True).start()
    threading.Thread(target=net_loop, daemon=True).start()
    q = queue.Queue()
    threading.Thread(target=stream, args=(q,), daemon=True).start()
    log("reveil-tour demarre. Appli coureur (marche sans reseau) : http://localhost:%d/coureur.html" % PORT)
    if live.session_id:
        log("reprise de la sortie", live.session_id, "-", len(live.points), "points deja enregistres")

    last_status = time.time()
    gps_silent_logged = False
    last_fix_t = 0.0
    last_good = None
    armed = False
    min_dist = None
    min_dist_t = None
    last_lap_t = max(live.laps) / 1000.0 if live.laps else 0.0

    while True:
        now = time.time()

        # quelle sortie ? la page coureur ouverte fait foi (elle marche hors ligne) ; sinon
        # Firestore quand il y a du reseau ; sinon la derniere connue.
        with live.lock:
            if now - live.page_heard < PAGE_TRUST_S:
                wanted, started = live.page_session, live.page_started
            elif live.remote is not None:
                wanted, started = live.remote
            else:
                wanted, started = live.session_id, live.started
            if wanted != live.session_id:
                live.switch(wanted)
                switched = True
            else:
                switched = False
            if started != live.started:
                live.started = started
                live.save()
        if switched:
            wake_lock(bool(wanted))
            armed, min_dist, min_dist_t, last_good = False, None, None, None
            last_lap_t = max(live.laps) / 1000.0 if live.laps else 0.0
            log("sortie active :", wanted or "aucune")
        session_id = live.session_id
        started_s = live.started / 1000.0 if live.started else None

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
                live.add_point({"t": int(fix_t * 1000), "lat": lat, "lon": lon, "a": int(round(acc or 0))})

                # pas de tour tant que la course n'a pas demarre (echauffement autour du depart)
                d = haversine(lat, lon, LAT0, LON0)
                if started_s is None or fix_t < started_s:
                    armed, min_dist, min_dist_t = False, None, None
                elif not armed and d > FAR_M:
                    armed, min_dist, min_dist_t = True, None, None
                if armed:
                    if min_dist is None or d < min_dist:
                        min_dist, min_dist_t = d, fix_t
                    lap_t = None
                    if d <= ENTER_M:
                        lap_t = fix_t
                    elif min_dist <= CAPTURE_M and d > min_dist + MOVE_AWAY_M:
                        lap_t = min_dist_t
                    if lap_t is not None and lap_t - max(last_lap_t, started_s) > MIN_LAP_S:
                        armed, min_dist, min_dist_t = False, None, None
                        last_lap_t = lap_t
                        with live.lock:
                            live.laps.append(int(lap_t * 1000))
                            live.pending_laps.append(int(lap_t * 1000))
                            live.save()
                        log("tour detecte")

        # GPS muet : on le signale dans Termux (cause probable : Android a mis la localisation de
        # Termux:API en pause, ecran eteint ou economie d'energie)
        if session_id and last_fix_t:
            silent = now - last_fix_t
            if silent > 90 and not gps_silent_logged:
                gps_silent_logged = True
                log("ATTENTION : plus de position GPS depuis", int(silent), "s.")
                log("  Dehors et ca dure ? Reglages Android > Applis > Termux:API > Forcer l'arret (le script continue).")
            elif silent <= 90 and gps_silent_logged:
                gps_silent_logged = False
                log("GPS revenu")

        if session_id and now - last_status >= 300:
            last_status = now
            with live.lock:
                waiting = sum(1 for p in live.points if p["t"] > live.uploaded_until)
            log("ok :", len(live.points), "points,", len(live.laps), "tours auto,", waiting, "points en attente de reseau")


if __name__ == "__main__":
    main()
