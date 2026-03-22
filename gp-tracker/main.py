import os
import httpx
import asyncio
from datetime import date
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db, save_snapshot, get_progress

COMLINK_URL = os.getenv("COMLINK_URL", "http://localhost:8080")
GUILD_ID = os.getenv("GUILD_ID", "fJXYTxpsS9iZvGj2M1OUGw")

async def fetch_guild_gp():
    print(f"[{date.today()}] Начинаем сбор данных по гильдии...")
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(f"{COMLINK_URL}/guild", json={
            "payload": {"guildId": GUILD_ID},
            "enums": False
        })
        r.raise_for_status()
        guild_data = r.json()

        members = guild_data.get("guild", {}).get("member", [])
        print(f"Найдено игроков: {len(members)}")

        players = []
        for member in members:
            player_id = member.get("playerId")
            name = member.get("playerName") or member.get("name") or "Unknown"
            if not player_id:
                print(f"  Пропускаем — нет playerId: {member}")
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
                print(f"  {name}: {total_gp:,} GP")
            except Exception as e:
                print(f"  Ошибка для {player_id}: {e}")
            await asyncio.sleep(0.2)

        players_with_gp = [p for p in players if p["gp"] > 0]
        if players_with_gp:
            save_snapshot(players_with_gp)
            print(f"Сохранено {len(players_with_gp)} игроков.")
        else:
            print("GP не найден ни у одного игрока.")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(fetch_guild_gp, "cron", hour=6, minute=0)
    scheduler.start()
    print("Планировщик запущен. Сбор данных каждый день в 06:00.")
    from database import is_empty
    if is_empty():
        print("База пустая — собираем данные сейчас...")
        asyncio.create_task(fetch_guild_gp())
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return FileResponse("static/index.html")

@app.get("/api/progress")
async def progress():
    data = get_progress()
    return data

@app.post("/api/collect")
async def collect():
    asyncio.create_task(fetch_guild_gp())
    return {"status": "started"}
    asyncio.create_task(fetch_guild_gp())
    return {"status": "started"}
