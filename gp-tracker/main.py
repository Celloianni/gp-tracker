import os
import httpx
import asyncio
from datetime import date
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
import secrets
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db, save_snapshot, get_progress, is_empty, get_friends_history

COMLINK_URL = os.getenv("COMLINK_URL", "http://localhost:8080")
COLLECT_PASSWORD = os.getenv("COLLECT_PASSWORD", "")
SITE_PASSWORD = os.getenv("SITE_PASSWORD", "")

class AuthChecker:
    async def __call__(self, request: Request):
        token = request.cookies.get("auth_token", "")
        if not SITE_PASSWORD or not secrets.compare_digest(token, SITE_PASSWORD):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return True

check_auth = AuthChecker()

GUILDS = [
    {"id": "fJXYTxpsS9iZvGj2M1OUGw", "name": "CAW Patrol"},
    {"id": "Iz6yUJtEQ-KS9yJyA_X4KA", "name": "Sigma Alliance Mandalorians"},
    {"id": "uebXPemvSvWX7entjA_F0g",  "name": "UA Rogu One"},
    {"id": "iLBJFQMpScuC44QMN9Bc9A",  "name": "Last Crusaders"},
    {"id": "T9bd5SpmRCal4_S80WSg6g",  "name": "IOuter RimI UA Mandalorians"},
    {"id": "bfAz1qGUR8mMkeGoXmZ6Mg",  "name": "IOuter RimI Malachor"},
]

FRIENDS = [
    {"allyCode": "151255425", "name": "Vesnar Keke"},
    {"allyCode": "374237198", "name": "Ø RƗΣ derentis"},
    {"allyCode": "889211288", "name": "KevinCrysler"},
    {"allyCode": "748519639", "name": "UKRAINE Renovatio"},
    {"allyCode": "722454792", "name": "Ø RƗΣ ψhαяρσση"},
    {"allyCode": "477916262", "name": "Ø RƗΣ Mҽɾƈҽɳαɾყ"},
]

# Global collection status
collection_status = {
    "running": False,
    "current": "",
    "done": 0,
    "total": 0,
}

async def fetch_player_by_allycode(client, ally_code: str, fallback_name: str):
    try:
        pr = await client.post(f"{COMLINK_URL}/player", json={
            "payload": {"allyCode": ally_code},
            "enums": False
        })
        pr.raise_for_status()
        pdata = pr.json()
        total_gp = 0
        for stat in pdata.get("profileStat", []):
            if stat.get("nameKey") == "STAT_GALACTIC_POWER_ACQUIRED_NAME":
                total_gp = int(float(stat.get("value", 0)))
                break
        name = pdata.get("name") or fallback_name
        player_id = pdata.get("playerId") or ally_code
        return {"id": player_id, "name": name, "gp": total_gp}
    except Exception as e:
        print(f"  Error fetching {fallback_name}: {e}")
        return None

async def fetch_friends():
    global collection_status
    print(f"[{date.today()}] Collecting friends data...")
    collection_status["current"] = "Friends"
    collection_status["done"] = 0
    collection_status["total"] += len(FRIENDS)
    async with httpx.AsyncClient(timeout=60) as client:
        players = []
        for f in FRIENDS:
            result = await fetch_player_by_allycode(client, f["allyCode"], f["name"])
            if result and result["gp"] > 0:
                players.append(result)
                print(f"  {result['name']}: {result['gp']:,} GP")
            collection_status["done"] += 1
            await asyncio.sleep(0.3)
        if players:
            save_snapshot("friends", players)
            print(f"  Saved {len(players)} friends.")

async def fetch_guild(client, guild):
    global collection_status
    guild_id = guild["id"]
    guild_name = guild["name"]
    print(f"  Collecting {guild_name}...")
    collection_status["current"] = guild_name
    try:
        r = await client.post(f"{COMLINK_URL}/guild", json={
            "payload": {"guildId": guild_id},
            "enums": False
        })
        r.raise_for_status()
        members = r.json().get("guild", {}).get("member", [])
    except Exception as e:
        print(f"  Error fetching guild {guild_name}: {e}")
        return

    collection_status["total"] += len(members)
    players = []
    for member in members:
        player_id = member.get("playerId")
        name = member.get("playerName") or member.get("name") or "Unknown"
        if not player_id:
            continue
        try:
            pr = await client.post(f"{COMLINK_URL}/player", json={
                "payload": {"playerId": player_id},
                "enums": False
            })
            pr.raise_for_status()
            pdata = pr.json()
            total_gp = 0
            for stat in pdata.get("profileStat", []):
                if stat.get("nameKey") == "STAT_GALACTIC_POWER_ACQUIRED_NAME":
                    total_gp = int(float(stat.get("value", 0)))
                    break
            name = pdata.get("name") or name
            players.append({"id": player_id, "name": name, "gp": total_gp})
        except Exception as e:
            print(f"    Error for {player_id}: {e}")
        collection_status["done"] += 1
        await asyncio.sleep(0.2)

    players_with_gp = [p for p in players if p["gp"] > 0]
    if players_with_gp:
        save_snapshot(guild_id, players_with_gp)
        print(f"  Saved {len(players_with_gp)} players for {guild_name}")

async def fetch_all():
    global collection_status
    collection_status["running"] = True
    collection_status["done"] = 0
    collection_status["total"] = 0
    collection_status["current"] = ""
    print(f"[{date.today()}] Starting full data collection...")
    await fetch_friends()
    async with httpx.AsyncClient(timeout=120) as client:
        for guild in GUILDS:
            await fetch_guild(client, guild)
    collection_status["running"] = False
    collection_status["current"] = "Done"
    print("Collection complete.")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(fetch_all, "cron", hour=6, minute=0)
    scheduler.start()
    print("Scheduler started. Data collection every day at 06:00.")
    if is_empty():
        print("Database empty — collecting now...")
        asyncio.create_task(fetch_all())
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/login")
async def login_page():
    return FileResponse("static/login.html")

@app.post("/login")
async def login(request: Request):
    body = await request.json()
    password = body.get("password", "")
    if not secrets.compare_digest(password, SITE_PASSWORD):
        raise HTTPException(status_code=401, detail="Wrong password")
    response = JSONResponse({"status": "ok"})
    response.set_cookie("auth_token", SITE_PASSWORD, httponly=True, samesite="strict")
    return response

@app.get("/")
async def index(auth: bool = Depends(check_auth)):
    return FileResponse("static/index.html")

@app.get("/api/guilds")
async def guilds(auth: bool = Depends(check_auth)):
    return [{"id": g["id"], "name": g["name"]} for g in GUILDS]

@app.get("/api/progress/{guild_id:path}")
async def progress(guild_id: str, auth: bool = Depends(check_auth)):
    return get_progress(guild_id)

@app.get("/api/friends/history")
async def friends_history(auth: bool = Depends(check_auth)):
    friend_ids = [f["allyCode"] for f in FRIENDS]
    return get_friends_history(friend_ids)

@app.get("/api/status")
async def status(auth: bool = Depends(check_auth)):
    s = collection_status
    pct = round(s["done"] / s["total"] * 100) if s["total"] > 0 else 0
    return {
        "running": s["running"],
        "current": s["current"],
        "done": s["done"],
        "total": s["total"],
        "pct": pct,
    }

@app.post("/api/collect")
async def collect(request: Request, auth: bool = Depends(check_auth)):
    body = await request.json()
    if body.get("password") != COLLECT_PASSWORD:
        raise HTTPException(status_code=401, detail="Wrong password")
    asyncio.create_task(fetch_all())
    return {"status": "started"}
