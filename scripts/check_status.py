#!/usr/bin/env python3
"""Polls vendor status feeds once and writes status/status.json. Stdlib only.
Run on a schedule by .github/workflows/status.yml."""
import json, os, sys, threading, time, time as _t
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = json.load(open(os.path.join(ROOT, "status", "services.json")))
OUT_FILE = os.path.join(ROOT, "status", "status.json")
HISTORY_MAX = 288  # ~24h at 5-minute runs
UA = "DistrictStatusPage/1.0"
RANK = {"operational": 0, "maintenance": 1, "degraded": 2, "outage": 3, "unknown": 1}

state, history, lock = {}, {}, threading.Lock()


def fetch_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def check_statuspage(svc):
    base = svc["url"].rstrip("/")
    d = fetch_json(base + "/api/v2/summary.json")
    ind = d["status"]["indicator"]
    status = {"none": "operational", "minor": "degraded", "major": "outage",
              "critical": "outage", "maintenance": "maintenance"}.get(ind, "unknown")
    comps = [{"name": c["name"], "status": {"operational": "operational", "degraded_performance": "degraded",
              "partial_outage": "degraded", "major_outage": "outage",
              "under_maintenance": "maintenance"}.get(c["status"], "unknown")}
             for c in d.get("components", []) if not c.get("group")]
    incidents = [{"name": i["name"], "status": i["status"], "url": i.get("shortlink") or base}
                 for i in d.get("incidents", [])]
    # Component-level problems with no headline indicator still count as degraded
    if status == "operational" and any(c["status"] in ("degraded", "outage") for c in comps):
        status = "degraded"
    return {"status": status, "message": d["status"]["description"], "incidents": incidents,
            "components": [c for c in comps if c["status"] != "operational"], "link": base}


def check_google(svc):
    d = fetch_json("https://www.google.com/appsstatus/dashboard/incidents.json")
    active = [i for i in d if not i.get("end")]
    incidents, status = [], "operational"
    for i in active:
        impact = str(i.get("status_impact", "")).upper()
        s = "outage" if "OUTAGE" in impact else "degraded"
        if RANK[s] > RANK[status]:
            status = s
        products = ", ".join(p.get("title", "") for p in i.get("affected_products", []))
        desc = (i.get("external_desc") or "").replace("**Summary:**", "").strip().split("\n")[0]
        incidents.append({"name": (products + ": " if products else "") + desc[:140],
                          "status": "ongoing", "url": svc["link"]})
    return {"status": status, "message": "All services available" if not active
            else f"{len(active)} active incident(s)", "incidents": incidents,
            "components": [], "link": svc["link"]}


def check_statusio(svc):
    d = fetch_json("https://api.status.io/1.0/status/" + svc["page_id"])["result"]
    code = d["status_overall"]["status_code"]
    status = {100: "operational", 200: "maintenance", 300: "degraded", 400: "degraded"}.get(code, "outage")
    comps = []
    for grp in d.get("status", []):
        for cont in grp.get("containers", []):
            if cont.get("status_code", 100) != 100:
                s = {200: "maintenance", 300: "degraded", 400: "degraded"}.get(cont["status_code"], "outage")
                comps.append({"name": grp["name"] + " - " + cont["name"], "status": s})
    incidents = [{"name": i.get("name", "Incident"), "status": "ongoing", "url": svc["link"]}
                 for i in d.get("incidents", [])]
    return {"status": status, "message": d["status_overall"]["status"], "incidents": incidents,
            "components": comps, "link": svc["link"]}


def check_instatus(svc):
    base = svc["url"].rstrip("/")
    st = fetch_json(base + "/api/v2/summary.json")["page"]["status"]
    status = {"UP": "operational", "UNDERMAINTENANCE": "maintenance"}.get(st, "degraded" if st == "HASISSUES" else "unknown")
    comps = []
    if status != "operational":
        for c in fetch_json(base + "/api/v2/components.json").get("components", []):
            s = {"OPERATIONAL": "operational", "UNDERMAINTENANCE": "maintenance", "MAJOROUTAGE": "outage"}.get(c["status"], "degraded")
            if s != "operational":
                comps.append({"name": c["name"], "status": s})
        if any(c["status"] == "outage" for c in comps):
            status = "outage"
    msg = {"operational": "All Systems Operational", "maintenance": "Under maintenance"}.get(status, "Experiencing issues")
    return {"status": status, "message": msg, "incidents": [], "components": comps, "link": base}


def check_http(svc):
    t0 = _t.time()
    req = urllib.request.Request(svc["url"], headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            code = r.status
    except urllib.error.HTTPError as e:
        code = e.code
    ms = int((_t.time() - t0) * 1000)
    if code >= 500:
        status, msg = "outage", f"Website returned HTTP {code}"
    elif ms > 4000:
        status, msg = "degraded", f"Responding slowly ({ms} ms)"
    else:
        status, msg = "operational", f"Website reachable ({ms} ms)"
    return {"status": status, "message": msg, "incidents": [], "components": [],
            "link": svc.get("link") or svc["url"], "latency_ms": ms, "note": "Website reachability check (no public status feed)"}


CHECKS = {"statusio": check_statusio, "instatus": check_instatus, "statuspage": check_statuspage, "google": check_google, "http": check_http}


def run_check(svc):
    try:
        try:
            res = CHECKS[svc["type"]](svc)
        except Exception:
            time.sleep(2)  # one retry: vendor feeds occasionally time out
            res = CHECKS[svc["type"]](svc)
    except Exception as e:
        res = {"status": "outage" if svc["type"] == "http" else "unknown",
               "message": f"Check failed: {type(e).__name__}", "incidents": [], "components": [],
               "link": svc.get("url") or svc.get("link")}
    res.update(id=svc["id"], name=svc["name"], desc=svc.get("desc", ""), logo=svc.get("logo", ""), type=svc["type"], checked=int(time.time()))
    with lock:
        state[svc["id"]] = res
        h = history.setdefault(svc["id"], [])
        h.append([res["checked"], res["status"]])
        del h[:-HISTORY_MAX]



def uptime(h):
    known = [s for _, s in h if s != "unknown"]
    return round(100 * sum(s in ("operational", "maintenance") for s in known) / len(known), 2) if known else None


def main():
    prev = {}
    if os.path.exists(OUT_FILE):
        try:
            prev = json.load(open(OUT_FILE))
        except Exception:
            pass
    for sid, h in prev.get("history", {}).items():
        history[sid] = h
    with ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(run_check, CONFIG["services"]))
    out = {"generated": int(time.time()),
           "services": [dict(state[s["id"]], uptime=uptime(history[s["id"]])) for s in CONFIG["services"]],
           "history": history}
    # Skip the write (and so the commit) when nothing visible changed and the file is < 30 min old.
    def sig(d):
        return json.dumps([(s["id"], s["status"], s["message"].split(" (")[0], [i["name"] for i in s["incidents"]])
                           for s in d.get("services", [])])
    if prev and sig(prev) == sig(out) and out["generated"] - prev.get("generated", 0) < 1800:
        print("No change; not writing")
        return
    json.dump(out, open(OUT_FILE, "w"), separators=(",", ":"))
    print("Wrote", OUT_FILE)


if __name__ == "__main__":
    main()
