import json
import time
import urllib.error
import urllib.request

base = "http://127.0.0.1:18080"


def req(method, path, obj=None):
    data = json.dumps(obj).encode() if obj is not None else None
    r = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else None
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body


print("health", req("GET", "/api/health")[1])

st, settings = req("GET", "/api/settings")
print("settings_get", st, settings)
settings["rate_limit_kbps"] = 2048
settings["max_concurrent"] = 3
settings["download_cover"] = True
settings["download_subs"] = True
st, saved = req("PUT", "/api/settings", settings)
print("settings_put", st, saved)
st, again = req("GET", "/api/settings")
print("settings_verify", st, again.get("rate_limit_kbps"), again.get("max_concurrent"))

st, created = req(
    "POST",
    "/api/tasks",
    {"url": "https://example.com/missing", "output_dir": "downloads/smoke3"},
)
print("create", st, created)
time.sleep(2)
st, tasks = req("GET", "/api/tasks")
for t in tasks or []:
    print(
        "task",
        t.get("status"),
        "title=",
        (t.get("title") or "")[:40],
        "can_retry=",
        t.get("can_retry"),
    )
    if t.get("can_retry") and t.get("status") == "failed":
        print("retry", req("POST", f"/api/tasks/{t['task_id']}/retry"))

print("escape", req("POST", "/api/tasks", {"url": "https://x.com", "output_dir": "../evil"}))
