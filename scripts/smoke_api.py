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
st, created = req(
    "POST", "/api/tasks", {"url": "https://example.com/missing", "output_dir": "downloads/smoke2"}
)
print("create", st, created)
time.sleep(2)
st, tasks = req("GET", "/api/tasks")
print("tasks", st)
for t in tasks or []:
    print(
        " -",
        t.get("task_id"),
        t.get("status"),
        "title=",
        (t.get("title") or "")[:50],
        "can_retry=",
        t.get("can_retry"),
        "err=",
        (t.get("error_msg") or "")[:100],
    )
    if t.get("can_retry") and t.get("status") == "failed":
        st2, r2 = req("POST", f"/api/tasks/{t['task_id']}/retry")
        print("retry", st2, r2)

print("escape", req("POST", "/api/tasks", {"url": "https://x.com", "output_dir": "../evil"}))
