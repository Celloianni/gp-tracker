import os
import httpx
import asyncio
from datetime import date
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db, save_snapshot, get_progress, is_empty

COMLINK_URL = os.getenv("COMLINK_URL", "http://localhost:8080")
COLLECT_PASSWORD = os.getenv("COLLECT_PASSWORD", "")

GUILDS = [
    {"id": "fJXYTxpsS9iZvGj2M1OUGw", "name": "CAW Patrol"},
    {"id": "Iz6yUJtEQ-KS9yJyA_X4KA", "name": "Sigma Alliance Mandalorians"},
    {"id": "uebXPemvSvWX7entjA_F0g",  "name": "UA Rogu One"},
    {"id": "iLBJFQMpScuC44QMN9Bc9A",  "name": "Last Crusaders"},
    {"id": "T9bd5SpmRCal4_S80WSg6g",  "name": "IOuter RimI UA Mandalorians"},
    {"id": "bfAz1qGUR8mMkeGoXmZ6Mg",  "name": "IOuter RimI Malachor"},
]

async def fetch_guild(client, guild):
    guild_id = guild["id"]
    guild_name = guild["name"]
    print(f"  Collecting {guild_name}...")
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
        await asyncio.sleep(0.2)

    players_with_gp = [p for p in players if p["gp"] > 0]
    if players_with_gp:
        save_snapshot(guild_id, players_with_gp)
        print(f"  Saved {len(players_with_gp)} players for {guild_name}")

async def fetch_all_guilds():
    print(f"[{date.today()}] Starting data collection for all guilds...")
    async with httpx.AsyncClient(timeout=120) as client:
        for guild in GUILDS:
            await fetch_guild(client, guild)
    print("Collection complete.")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(fetch_all_guilds, "cron", hour=6, minute=0)
    scheduler.start()
    print("Scheduler started. Data collection every day at 06:00.")
    if is_empty():
        print("Database empty — collecting now...")
        asyncio.create_task(fetch_all_guilds())
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.get("/api/guilds")
async def guilds():
    return [{"id": g["id"], "name": g["name"]} for g in GUILDS]

@app.get("/api/progress/{guild_id}")
async def progress(guild_id: str):
    return get_progress(guild_id)

@app.post("/api/collect")
async def collect(request: Request):
    body = await request.json()
    if body.get("password") != COLLECT_PASSWORD:
        raise HTTPException(status_code=401, detail="Wrong password")
    asyncio.create_task(fetch_all_guilds())
    return {"status": "started"}
