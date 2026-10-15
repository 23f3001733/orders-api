import base64
import math
import threading
import time
import uuid
from collections import deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 42      # your T
RATE = 17       # your R
WINDOW = 10     # seconds

app = FastAPI()
lock = threading.Lock()

# ---------- 3. Rate limiting (each client has its own list of timestamps) ----------
hits = {}

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client = request.headers.get("x-client-id")
    if client and request.method != "OPTIONS":
        now = time.monotonic()
        with lock:
            q = hits.setdefault(client, deque())
            while q and now - q[0] >= WINDOW:   # forget old requests
                q.popleft()
            if len(q) >= RATE:                  # too many -> 429
                wait = max(1, math.ceil(WINDOW - (now - q[0])))
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too Many Requests"},
                    headers={"Retry-After": str(wait)},
                )
            q.append(now)
    return await call_next(request)

# CORS must be added AFTER the middleware above so it wraps it (even 429s get CORS headers)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# ---------- 1. Idempotent POST ----------
orders_by_key = {}

@app.post("/orders")
async def create_order(request: Request):
    key = request.headers.get("idempotency-key")
    if not key:
        return JSONResponse(status_code=400,
                            content={"detail": "Idempotency-Key header required"})
    with lock:
        if key not in orders_by_key:           # first time -> create
            orders_by_key[key] = {"id": str(uuid.uuid4()), "status": "created"}
        order = orders_by_key[key]             # repeat -> same order
    return JSONResponse(status_code=201, content=order)

# ---------- 2. Cursor pagination ----------
def encode(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()

def decode(cursor: str) -> int:
    try:
        return max(0, int(base64.urlsafe_b64decode(cursor.encode()).decode()[2:]))
    except Exception:
        return 0

@app.get("/orders")
async def list_orders(limit: int = 10, cursor: str = None):
    limit = max(1, min(limit, TOTAL))
    start = decode(cursor) if cursor else 0
    end = min(start + limit, TOTAL)
    items = [{"id": i, "status": "created"} for i in range(start + 1, end + 1)]
    next_cursor = encode(end) if end < TOTAL else None
    return {"items": items, "next_cursor": next_cursor}

@app.get("/")
async def root():
    return {"status": "ok"}
