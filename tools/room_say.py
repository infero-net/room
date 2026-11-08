#!/usr/bin/env python3
"""在群里说一句（整条）。用法：room_say.py <我的账号> "文字"   或   echo 文字 | room_say.py <我的账号> -"""
import base64, json, os, sys, urllib.request, uuid
R = os.environ.get("ROOM", "http://127.0.0.1:8951")
user, text = sys.argv[1], sys.argv[2]
if text == "-":
    text = sys.stdin.read()
auth = os.environ.get("ROOM_AUTH", f"{user}:x")
req = urllib.request.Request(R + "/send", data=json.dumps({"text": text, "id": uuid.uuid4().hex[:16]}).encode(),
                             headers={"Content-Type": "application/json", "Authorization": "Basic " + base64.b64encode(auth.encode()).decode()})
print(urllib.request.urlopen(req, timeout=30).read().decode())
