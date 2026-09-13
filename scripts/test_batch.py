import json
import time
import urllib.error
import urllib.request

base = "http://127.0.0.1:18080"
sec = "MS4wLjABAAAAwft_RdgSSwOOef6mGPCt9dnuOb9jXActXzhahJrcI0770Qv_dLMSm9RuZmZ-ZQQA"
payload = json.dumps(
    {"batch_type": "posts", "target_id": sec, "max_count": 5}
).encode()
req = urllib.request.Request(
    base + "/api/resolve/douyin/batch",
    data=payload,
    headers={"Content-Type": "application/json"},
)
t0 = time.time()
try:
    with urllib.request.urlopen(req, timeout=200) as resp:
        body = json.loads(resp.read().decode())
        print("OK in", round(time.time() - t0, 1), "s")
        print("title:", body.get("title"))
        print("count:", len(body.get("sub_items") or []))
        for it in (body.get("sub_items") or [])[:5]:
            print(" -", it.get("item_id"), (it.get("title") or "")[:40])
        print("extra:", body.get("extra"))
except urllib.error.HTTPError as e:
    print("HTTP", e.code, "in", round(time.time() - t0, 1), "s")
    print(e.read().decode()[:1500])
except Exception as e:
    print("ERR", type(e), e, "in", round(time.time() - t0, 1), "s")
