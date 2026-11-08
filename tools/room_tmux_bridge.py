#!/usr/bin/env python3
"""常驻：群里有别人的新完整消息 → 在 tmux 面板里敲一行“【群聊】…”叫醒那里的 CC 会话。
用法：room_tmux_bridge.py <账号> <tmux 面板，如 0:0.0>
- 读到哪儿记在 ~/.room_seen_<账号>（首次从当前末尾开始，不补历史）；只在敲成功后才推进，重启不漏、不重报。
- 同一批消息等 QUIET 秒安静后合并成一行；换行变成 ⏎，太长截断并给出全文的取法。
- 回复由会话自己用 room_say.py 发。"""
import base64, json, os, subprocess, sys, time, urllib.request
R = os.environ.get("ROOM", "http://127.0.0.1:8951")
user, pane = sys.argv[1], sys.argv[2]
QUIET, PER, TOTAL = 4, 600, 1800
H = {"Authorization": "Basic " + base64.b64encode(f"{user}:x".encode()).decode()}
SEEN = os.path.expanduser(f"~/.room_seen_{user}")
TOOLS = os.path.dirname(os.path.abspath(__file__))


def get(path):
    return json.load(urllib.request.urlopen(urllib.request.Request(R + path, headers=H), timeout=30))


def pending():
    h = get("/history")
    me, msgs = h["me"], h["msgs"]
    ids = [m["id"] for m in msgs]
    if not os.path.exists(SEEN):
        open(SEEN, "w").write(ids[-1] if ids else "")
    last = open(SEEN).read().strip()
    new = msgs[ids.index(last) + 1:] if last in ids else []
    return me, new, (ids[-1] if ids else last)


def ring(me, new, tail):
    others = [m for m in new if m["who"] != me]
    if others:
        parts, size = [], 0
        for m in others:
            t = m["text"].replace("\r", "").replace("\n", " ⏎ ")
            if len(t) > PER:
                t = t[:PER] + f"…（全文 {len(m['text'])} 字）"
            parts.append(f"{m['who']}：{t}")
        line = "【群聊】" + " ‖ ".join(parts)
        if len(line) > TOTAL:
            line = line[:TOTAL] + "…"
        line += f"（回复：{TOOLS}/room_say.py {user} '…'；全文 curl -s {R}/history；不需要就不回）"
        ok = subprocess.run(["tmux", "send-keys", "-t", pane, "-l", line]).returncode == 0
        time.sleep(1)
        ok = ok and subprocess.run(["tmux", "send-keys", "-t", pane, "Enter"]).returncode == 0
        if not ok:
            print("RING FAILED", flush=True)
            return
        print(time.strftime("%H:%M:%S"), "rang", len(others), flush=True)
    open(SEEN, "w").write(tail)


while True:
    try:
        me, new, tail = pending()
        if new:
            ring(me, new, tail)
        with urllib.request.urlopen(urllib.request.Request(R + "/events", headers=H), timeout=90) as r:
            hot = 0.0
            for line in r:
                if line.startswith(b"data: "):
                    ev = json.loads(line[6:])
                    if ev.get("type") in ("msg", "done"):
                        hot = time.time()
                if hot and time.time() - hot >= QUIET:
                    break
            if hot:
                while time.time() - hot < QUIET:
                    time.sleep(0.5)
    except Exception as e:
        time.sleep(3)
