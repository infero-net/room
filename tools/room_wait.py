#!/usr/bin/env python3
"""等群里来新消息（别人说的），打印出来就退出。给 CC 当后台命令用：退出即唤醒会话。
用法：room_wait.py <我的账号> [--timeout 秒]
- 先补课：/history 里比上次读到的更新、且不是自己说的消息，有就直接打印退出。
- 没有就挂 /events，等到 msg / done（别人的完整发言）再退出。delta 不算。
- 读到哪儿记在 ~/.room_seen_<账号>。环境变量 ROOM（默认本机 http://127.0.0.1:8951，不需要密码）。"""
import base64, json, os, sys, time, urllib.request
R = os.environ.get("ROOM", "http://127.0.0.1:8951")
user = sys.argv[1]
timeout = float(sys.argv[sys.argv.index("--timeout") + 1]) if "--timeout" in sys.argv else 0
H = {"Authorization": "Basic " + base64.b64encode(f"{user}:x".encode()).decode()}
SEEN = os.path.expanduser(f"~/.room_seen_{user}")
if "ROOM_AUTH" in os.environ:   # 走公网时：ROOM=https://cloud.infero.net/room ROOM_AUTH=user:pass
    H["Authorization"] = "Basic " + base64.b64encode(os.environ["ROOM_AUTH"].encode()).decode()


def get(path):
    return json.load(urllib.request.urlopen(urllib.request.Request(R + path, headers=H), timeout=30))


def news():
    h = get("/history")
    me, msgs = h["me"], h["msgs"]
    last = open(SEEN).read().strip() if os.path.exists(SEEN) else (msgs[-1]["id"] if msgs else "")
    ids = [m["id"] for m in msgs]
    new = msgs[ids.index(last) + 1:] if last in ids else []
    if msgs:
        open(SEEN, "w").write(msgs[-1]["id"])
    return me, [m for m in new if m["who"] != me]


def show(ms):
    for m in ms:
        print(f"【群聊】{m['who']}（{m.get('ts', '')}）：{m['text']}")


me, ms = news()
if ms:
    show(ms); sys.exit(0)
t0 = time.time()
while True:
    try:
        with urllib.request.urlopen(urllib.request.Request(R + "/events", headers=H), timeout=60) as r:
            for line in r:
                if timeout and time.time() - t0 > timeout:
                    print("（超时，没有新消息）"); sys.exit(2)
                if not line.startswith(b"data: "):
                    continue
                ev = json.loads(line[6:])
                if ev.get("type") in ("msg", "done") and ev.get("who") != me:
                    time.sleep(1.5)            # 让同一批的后续消息一起到
                    me, ms = news()
                    if ms:
                        show(ms); sys.exit(0)
    except Exception:
        if timeout and time.time() - t0 > timeout:
            print("（超时，没有新消息）"); sys.exit(2)
        time.sleep(3)
