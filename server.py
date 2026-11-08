#!/usr/bin/env python3
"""room：一个人和 being 混住的网页群聊。说明见 README.md。
- 身份 = 反向代理 basic auth 的用户名（这里只解析 Authorization 头，不验密码）。
- 消息存 room.jsonl；本地 agent（claude -p）会话 id 存 session.txt，每轮 --resume 接着聊。
- SSE /events 推送 msg / start / delta / tool / tool_in / tool_out / done / drop / state。"""
from urllib.parse import unquote
import asyncio, base64, json, os, re, time, uuid
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, FileResponse, Response

D = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(D, "room.jsonl")
SESS = os.path.join(D, "session.txt")
CFG = json.load(open(os.path.join(D, "config.json"), encoding="utf-8")) if os.path.exists(os.path.join(D, "config.json")) else {}
CWD = os.path.expanduser(CFG.get("cwd", "~"))          # 分身 claude -p 的工作目录（决定它读到哪份 CLAUDE.md）
NAMES = CFG.get("names", {})                           # basic auth 用户名 → 显示名
AGENT = CFG.get("agent", "agent")                      # 分身的显示名
SYS = CFG.get("sys", "你在一个网页群聊里。每条消息前面标了说话人。不需要你说话时就不输出任何文字。")
UP = os.path.join(D, "uploads")
FILE_RE = re.compile(r"\[[^\]\n]*\]\(file/([0-9a-f]{16})/([^\s)]+)\)")
IMG_RE = re.compile(r"!\[[^\]\n]*\]\(img/([0-9a-f]{16}\.(?:jpg|png|webp|gif))\)")


def sysprompt():
    try:
        return SYS + "\n\n" + open(os.path.join(D, "people.md"), encoding="utf-8").read()
    except Exception:
        return SYS

app = FastAPI()
subs: set[asyncio.Queue] = set()
pending: list[dict] = []
state = {"busy": False, "proc": None}
seen_ids: set[str] = set()


def load():
    if not os.path.exists(LOG):
        return []
    return [json.loads(l) for l in open(LOG, encoding="utf-8") if l.strip()]


def save(m):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(m, ensure_ascii=False) + "\n")


def who(req: Request):
    h = req.headers.get("authorization", "")
    try:
        u = base64.b64decode(h.split(" ", 1)[1]).decode().split(":", 1)[0]
    except Exception:
        u = "guest"
    return NAMES.get(u, u)


def clip(x, n=6000):
    return x if len(x) <= n else x[:n] + f"\n…（截断，共 {len(x)} 字）"


async def push(ev):
    for q in list(subs):
        q.put_nowait(ev)


async def run_agent():
    state["busy"] = True
    await push({"type": "state", "busy": True})
    try:
        while pending:
            batch = pending[:]
            pending.clear()
            prompt = "\n".join(f"【群聊】{m['who']}：" + FILE_RE.sub(lambda g: f"[文件：{os.path.join(UP, g.group(1), unquote(g.group(2)))}]", IMG_RE.sub(lambda g: f"[图片：{os.path.join(UP, g.group(1))}]", m['text'])) for m in batch)
            sid = open(SESS).read().strip() if os.path.exists(SESS) else None
            args = ["claude", "-p", prompt, "--output-format", "stream-json", "--verbose",
                    "--include-partial-messages", "--permission-mode", "bypassPermissions",
                    "--append-system-prompt", sysprompt()]
            if sid:
                args += ["--resume", sid]
            else:
                sid = str(uuid.uuid4())
                args += ["--session-id", sid]
                open(SESS, "w").write(sid)
            mid = uuid.uuid4().hex[:12]
            await push({"type": "start", "id": mid, "who": AGENT})
            proc = await asyncio.create_subprocess_exec(*args, cwd=CWD, stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.DEVNULL, limit=1 << 24)
            state["proc"] = proc
            text = ""
            tools = []
            async for line in proc.stdout:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("type") == "stream_event":
                    ev = e.get("event", {})
                    if ev.get("type") == "content_block_delta" and ev.get("delta", {}).get("type") == "text_delta":
                        t = ev["delta"]["text"]
                        text += t
                        await push({"type": "delta", "id": mid, "t": t})
                    elif ev.get("type") == "content_block_start" and ev.get("content_block", {}).get("type") == "tool_use":
                        cb = ev["content_block"]
                        tools.append({"tid": cb.get("id", ""), "name": cb.get("name", ""), "input": "", "out": ""})
                        await push({"type": "tool", "id": mid, "tid": cb.get("id", ""), "name": cb.get("name", "")})
                    elif ev.get("type") == "message_start" and text and not text.endswith("\n"):
                        text += "\n\n"
                        await push({"type": "delta", "id": mid, "t": "\n\n"})
                elif e.get("type") == "assistant":
                    for c in e.get("message", {}).get("content", []) or []:
                        if c.get("type") == "tool_use":
                            inp = c.get("input", {})
                            s_in = inp.get("command") if isinstance(inp, dict) and "command" in inp else json.dumps(inp, ensure_ascii=False, indent=1)
                            s_in = clip(s_in)
                            for t in tools:
                                if t["tid"] == c.get("id"):
                                    t["input"] = s_in
                                    t["desc"] = inp.get("description", "") if isinstance(inp, dict) else ""
                            await push({"type": "tool_in", "id": mid, "tid": c.get("id", ""), "input": s_in,
                                        "desc": inp.get("description", "") if isinstance(inp, dict) else ""})
                elif e.get("type") == "user":
                    for c in e.get("message", {}).get("content", []) or []:
                        if isinstance(c, dict) and c.get("type") == "tool_result":
                            r = c.get("content", "")
                            if isinstance(r, list):
                                r = "\n".join(x.get("text", "") for x in r if isinstance(x, dict))
                            r = clip(str(r))
                            for t in tools:
                                if t["tid"] == c.get("tool_use_id"):
                                    t["out"] = r
                            await push({"type": "tool_out", "id": mid, "tid": c.get("tool_use_id", ""), "out": r})
            await proc.wait()
            state["proc"] = None
            if not text.strip() and not tools:
                await push({"type": "drop", "id": mid})   # 分身选择沉默：群里不留消息
                continue
            m = {"id": mid, "who": AGENT, "text": text.strip(), "ts": time.strftime("%H:%M"), "tools": tools}
            save(m)
            await push({"type": "done", **m})
    finally:
        state["busy"] = False
        await push({"type": "state", "busy": False})


@app.get("/", response_class=HTMLResponse)
async def index():
    return open(os.path.join(D, "index.html"), encoding="utf-8").read()


@app.get("/README.md")
async def readme():
    return FileResponse(os.path.join(D, "README.md"), media_type="text/markdown; charset=utf-8")


@app.get("/src/{name}")
async def src(name: str):
    if name not in ("server.py", "index.html", "README.md"):
        return Response(status_code=404)
    return FileResponse(os.path.join(D, name), media_type="text/plain; charset=utf-8")


@app.get("/history")
async def history(req: Request):
    return JSONResponse({"me": who(req), "msgs": load()[-300:], "busy": state["busy"], "agent": AGENT, "title": CFG.get("title", "room")})


@app.post("/send")
async def send(req: Request):
    body = await req.json()
    t = (body.get("text") or "").strip()
    if not t:
        return {"ok": False}
    cid = str(body.get("id") or "")
    if cid and (not cid.isalnum() or len(cid) > 24):
        cid = ""
    if cid and cid in seen_ids:
        return {"ok": True, "dup": True}
    m = {"id": cid or uuid.uuid4().hex[:12], "who": who(req), "text": t, "ts": time.strftime("%H:%M")}
    if cid:
        seen_ids.add(cid)
    save(m)
    await push({"type": "msg", **m})
    pending.append(m)
    if not state["busy"]:
        asyncio.create_task(run_agent())
    return {"ok": True}


@app.post("/upload")
async def upload(req: Request):
    body = await req.json()
    d = body.get("data", "")
    if "," in d:
        d = d.split(",", 1)[1]
    try:
        raw = base64.b64decode(d)
    except Exception:
        return JSONResponse({"ok": False}, status_code=400)
    if len(raw) > 30 << 20:
        return JSONResponse({"ok": False, "err": "too big"}, status_code=413)
    ext = ("jpg" if raw[:3] == b"\xff\xd8\xff" else "png" if raw[:8] == b"\x89PNG\r\n\x1a\n"
           else "webp" if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP" else "gif" if raw[:4] == b"GIF8" else "")
    if not ext or body.get("kind") == "file":
        if len(raw) > 30 << 20:
            return JSONResponse({"ok": False, "err": "too big"}, status_code=413)
        fn = re.sub(r'[\x00-\x1f/\\:*?"<>|]', "_", str(body.get("name") or "file")).strip(". ")[:120] or "file"
        fid = uuid.uuid4().hex[:16]
        os.makedirs(os.path.join(UP, fid), exist_ok=True)
        open(os.path.join(UP, fid, fn), "wb").write(raw)
        return {"ok": True, "url": f"file/{fid}/{fn}", "name": fn}
    os.makedirs(UP, exist_ok=True)
    name = uuid.uuid4().hex[:16] + "." + ext
    open(os.path.join(UP, name), "wb").write(raw)
    return {"ok": True, "url": "img/" + name}


@app.get("/img/{name}")
async def img(name: str):
    if not re.fullmatch(r"[0-9a-f]{16}\.(jpg|png|webp|gif)", name) or not os.path.exists(os.path.join(UP, name)):
        return Response(status_code=404)
    return FileResponse(os.path.join(UP, name), headers={"Cache-Control": "private, max-age=31536000"})


@app.get("/file/{fid}/{fn}")
async def file_dl(fid: str, fn: str):
    p = os.path.join(UP, fid, fn)
    if not re.fullmatch(r"[0-9a-f]{16}", fid) or "/" in fn or fn.startswith(".") or not os.path.isfile(p):
        return Response(status_code=404)
    return FileResponse(p, filename=fn)


@app.post("/stop")
async def stop():
    p = state.get("proc")
    if p and p.returncode is None:
        p.terminate()
    return {"ok": True}


@app.get("/events")
async def events(req: Request):
    q: asyncio.Queue = asyncio.Queue()
    subs.add(q)

    async def gen():
        try:
            yield "retry: 2000\n\n"
            while True:
                if await req.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), 15)
                    yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            subs.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
