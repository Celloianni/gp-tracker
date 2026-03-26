import os
import httpx
import asyncio
from datetime import date
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse as FastAPIFileResponse
import secrets
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db, save_snapshot, get_progress, is_empty, get_friends_history, get_available_months, get_progress_for_month, get_monthly_progress, get_setting, set_setting, get_monthly_achievements, save_roster_snapshot, save_unit_names, get_unit_names_count, get_all_unit_ids, get_roster_dates, get_roster_changes

COMLINK_URL = os.getenv("COMLINK_URL", "http://localhost:8080")
COLLECT_PASSWORD = os.getenv("COLLECT_PASSWORD", "")
DB_PATH = "/data/gp_tracker.db"
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

def extract_roster_units(pdata: dict) -> list:
    """Extract minimal roster data from comlink player response."""
    units = []
    for unit in pdata.get("rosterUnit", []):
        def_id = unit.get("definitionId", "")
        # definitionId format: "DARTHREVAN:SEVEN_STAR" — take only part before ":"
        unit_id = def_id.split(":")[0] if ":" in def_id else def_id
        if not unit_id:
            continue
        relic_data = unit.get("relic", {})
        relic_tier = relic_data.get("currentTier", -1) if relic_data else -1
        units.append({
            "unit_id": unit_id,
            "level": unit.get("currentLevel", 1),
            "gear_tier": unit.get("currentTier", 1),
            "relic_tier": relic_tier if relic_tier is not None else -1,
            "stars": unit.get("currentStars", 1),
            "combat_type": unit.get("combatType", 1),
        })
    return units

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
        # Save roster snapshot
        units = extract_roster_units(pdata)
        if units:
            save_roster_snapshot(player_id, str(date.today()), units)
        return {"id": player_id, "name": name, "gp": total_gp}
    except Exception as e:
        print(f"  Error fetching {fallback_name}: {e}")
        return None

async def fetch_friends():
    global collection_status
    print(f"[{date.today()}] Collecting friends data...")
    collection_status["current"] = "Friends"
    collection_status["done"] = collection_status["done"]  # keep running total
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

    # total already estimated at start
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
    # Estimate total: 6 friends + 6 guilds x ~50 players
    estimated_total = len(FRIENDS) + len(GUILDS) * 50
    collection_status["running"] = True
    collection_status["done"] = 0
    collection_status["total"] = estimated_total
    collection_status["current"] = ""
    print(f"[{date.today()}] Starting full data collection...")
    await fetch_friends()
    async with httpx.AsyncClient(timeout=120) as client:
        for guild in GUILDS:
            await fetch_guild(client, guild)
    collection_status["running"] = False
    collection_status["current"] = "Done"
    collection_status["done"] = collection_status["total"]
    print("Collection complete.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    print("App started.")
    if is_empty():
        print("Database empty — collecting now...")
        asyncio.create_task(fetch_all())
    yield

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
async def index(request: Request):
    token = request.cookies.get("auth_token", "")
    if not SITE_PASSWORD or not secrets.compare_digest(token, SITE_PASSWORD):
        return RedirectResponse(url="/login")
    return FileResponse("static/index.html")

@app.get("/api/guilds")
async def guilds(auth: bool = Depends(check_auth)):
    return [{"id": g["id"], "name": g["name"]} for g in GUILDS]

@app.get("/api/progress/{guild_id:path}")
async def progress(guild_id: str, month: str = None, auth: bool = Depends(check_auth)):
    if month:
        return get_progress_for_month(guild_id, month)
    return get_monthly_progress(guild_id)

@app.get("/api/months/{guild_id:path}")
async def months(guild_id: str, auth: bool = Depends(check_auth)):
    return get_available_months(guild_id)

@app.get("/api/friends/history")
async def friends_history(auth: bool = Depends(check_auth)):
    friend_ids = [f["allyCode"] for f in FRIENDS]
    return get_friends_history(friend_ids)

@app.get("/api/friends/achievements")
async def friends_achievements(auth: bool = Depends(check_auth)):
    return get_monthly_achievements("friends")

@app.post("/api/cron")
async def cron_trigger(request: Request):
    token = request.headers.get("X-Cron-Token", "")
    cron_secret = os.getenv("CRON_SECRET", "")
    if not cron_secret or token != cron_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")
    asyncio.create_task(fetch_all())
    return {"status": "started"}

@app.get("/api/settings")
async def get_settings(auth: bool = Depends(check_auth)):
    return {"monthly_plan": int(get_setting("monthly_plan", "100000"))}

@app.post("/api/settings")
async def update_settings(request: Request, auth: bool = Depends(check_auth)):
    body = await request.json()
    if "monthly_plan" in body:
        plan = int(body["monthly_plan"])
        if plan < 1000 or plan > 10000000:
            raise HTTPException(status_code=400, detail="Plan must be between 1,000 and 10,000,000")
        set_setting("monthly_plan", str(plan))
    return {"status": "ok", "monthly_plan": int(get_setting("monthly_plan", "100000"))}

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

@app.get("/api/backup")
async def backup(request: Request):
    token = request.cookies.get("auth_token", "")
    if not SITE_PASSWORD or not secrets.compare_digest(token, SITE_PASSWORD):
        raise HTTPException(status_code=401, detail="Unauthorized")
    from datetime import date
    filename = f"gp_tracker_backup_{date.today()}.db"
    return FastAPIFileResponse(DB_PATH, filename=filename, media_type="application/octet-stream")

@app.post("/api/collect")
async def collect(request: Request, auth: bool = Depends(check_auth)):
    body = await request.json()
    if body.get("password") != COLLECT_PASSWORD:
        raise HTTPException(status_code=401, detail="Wrong password")
    asyncio.create_task(fetch_all())
    return {"status": "started"}

async def fetch_and_cache_unit_names():
    """Fetch unit names from swgoh-utils gamedata on GitHub and cache in DB."""
    LOC_URL = "https://raw.githubusercontent.com/swgoh-utils/gamedata/main/Loc_ENG_US.txt.json"
    print("Fetching unit names from swgoh-utils/gamedata GitHub...")

    known_ids = get_all_unit_ids()
    print(f"  Found {len(known_ids)} unique unit IDs in roster snapshots")

    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.get(LOC_URL)
        r.raise_for_status()
        loc_map = r.json().get("data", {})  # {"version": "...", "data": {"UNIT_HANSOLO_NAME": "Han Solo", ...}}
        print(f"  Loaded {len(loc_map)} localization strings")

        names_to_save = {}
        for unit_id in known_ids:
            name = loc_map.get(f"UNIT_{unit_id}_NAME", "")
            names_to_save[unit_id] = {
                "name": name if name else unit_id,
                "combat_type": 1
            }

        save_unit_names(names_to_save)
        print(f"  Cached {len(names_to_save)} unit names")
        return len(names_to_save)

@app.get("/player/{player_slug}")
async def player_page(player_slug: str, request: Request):
    token = request.cookies.get("auth_token", "")
    if not SITE_PASSWORD or not secrets.compare_digest(token, SITE_PASSWORD):
        return RedirectResponse(url="/login")
    return FileResponse("static/player.html")

@app.get("/api/friends/roster_changes/{player_id}")
async def roster_changes(player_id: str, date: str = None, auth: bool = Depends(check_auth)):
    return get_roster_changes(player_id, date)

@app.get("/api/friends/roster_dates/{player_id}")
async def roster_dates(player_id: str, auth: bool = Depends(check_auth)):
    return get_roster_dates(player_id)

@app.post("/api/admin/sync_unit_names")
async def sync_unit_names(request: Request, auth: bool = Depends(check_auth)):
    """Manually trigger unit names sync from comlink."""
    try:
        count = await fetch_and_cache_unit_names()
        return {"status": "ok", "count": count}
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"sync_unit_names error:\n{tb}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/unit_names_status")
async def unit_names_status(auth: bool = Depends(check_auth)):
    return {"count": get_unit_names_count()}

@app.get("/api/test/localization")
async def test_localization(auth: bool = Depends(check_auth)):
    """Test swgoh.gg API for unit names."""
    results = {}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get("https://swgoh.gg/api/characters/")
            chars = r.json()
            sample = chars[:3] if isinstance(chars, list) else chars
            results["characters"] = {"status": r.status_code, "count": len(chars) if isinstance(chars, list) else "?", "sample": sample}
        except Exception as e:
            results["characters"] = {"error": str(e)}
        try:
            r = await client.get("https://swgoh.gg/api/ships/")
            ships = r.json()
            sample = ships[:3] if isinstance(ships, list) else ships
            results["ships"] = {"status": r.status_code, "count": len(ships) if isinstance(ships, list) else "?", "sample": sample}
        except Exception as e:
            results["ships"] = {"error": str(e)}
    return results

@app.get("/api/friends/list")
async def friends_list(auth: bool = Depends(check_auth)):
    """Return friends list with ally codes for player page routing."""
    return [{"allyCode": f["allyCode"], "name": f["name"]} for f in FRIENDS]
