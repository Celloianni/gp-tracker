import os
import httpx
import asyncio
from datetime import date, datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse as FastAPIFileResponse
import secrets
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import init_db, save_snapshot, get_progress, is_empty, get_friends_history, get_available_months, get_progress_for_month, get_monthly_progress, get_setting, set_setting, get_monthly_achievements, save_roster_snapshot, save_unit_names, get_unit_names_count, get_all_unit_ids, get_all_ability_ids, get_roster_dates, get_roster_changes, get_roster_changes_for_month, get_player_gp_for_period

COMLINK_URL = os.getenv("COMLINK_URL", "http://localhost:8080")

EN_MONTHS = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}

def _fmt_change(c: dict) -> str:
    field = c.get("field", "")
    if field == "stars":
        return f"{c['from']}* -> {c['to']}*"
    if field == "level":
        return f"Level {c['from']} -> {c['to']}"
    if field == "gear_tier":
        return f"G{c['from']} -> G{c['to']}"
    if field == "relic_tier":
        from_r = "G13" if c.get("from", 0) <= 2 else f"R{c['from'] - 2}"
        to_r = f"R{c['to'] - 2}"
        return f"{from_r} -> {to_r}"
    if field == "ability":
        zeta = " [Zeta]" if c.get("is_zeta") else ""
        name = c.get("ability_name") or c.get("ability_id", "")
        return f"{name}{zeta}: Tier {c['from']} -> {c['to']}"
    if field == "ability_new":
        zeta = " [Zeta]" if c.get("is_zeta") else ""
        name = c.get("ability_name") or c.get("ability_id", "")
        return f"{name}{zeta}: new (Tier {c.get('tier', '?')})"
    return f"{field}: {c.get('from', '?')} -> {c.get('to', '?')}"


def _generate_month_block(player_name: str, player_id: str, year: int, month: int) -> str:
    import calendar as cal_mod
    SEP  = "=" * 80
    SEP2 = "-" * 80
    month_str   = f"{year}-{str(month).zfill(2)}"
    month_label = f"{EN_MONTHS[month]} {year}"
    last_day    = cal_mod.monthrange(year, month)[1]
    date_from   = f"{year}-{str(month).zfill(2)}-01"
    date_to     = f"{year}-{str(month).zfill(2)}-{str(last_day).zfill(2)}"

    gp = get_player_gp_for_period(player_id, date_from, date_to)
    month_data = get_roster_changes_for_month(player_id, month_str)

    lines = [SEP, f"  GP TRACKER -- {player_name}", f"  {month_label}", SEP]

    if gp["gp_start"] is not None and gp["gp_end"] is not None:
        diff = gp["gp_end"] - gp["gp_start"]
        diff_str = f"+{diff:,}" if diff >= 0 else f"{diff:,}"
        lines.append(f"GP at start of month:  {gp['gp_start']:,}")
        lines.append(f"GP at end of month:    {gp['gp_end']:,}")
        lines.append(f"Growth:                {diff_str}")
    else:
        lines.append("(No GP data for this month)")

    days_with_changes = []
    days_no_changes   = []
    for date_str in sorted(month_data.keys()):
        day_data = month_data[date_str]
        day_num  = int(date_str.split("-")[2])
        upgrades = [c for c in day_data.get("changes", []) if c.get("type") == "upgrade"]
        if upgrades:
            days_with_changes.append((day_num, date_str, upgrades))
        elif day_data.get("has_prev"):
            days_no_changes.append(day_num)

    for day_num, date_str, upgrades in days_with_changes:
        lines.append(SEP2)
        lines.append(f"  {EN_MONTHS[month]} {day_num}, {year}")
        lines.append(SEP2)
        for change in upgrades:
            labels = ", ".join(_fmt_change(c) for c in change.get("changes", []))
            lines.append(f"  {change['name']:<32} {labels}")

    lines.append(SEP2)
    if days_no_changes:
        lines.append(f"  Days with no upgrades: {', '.join(str(d) for d in sorted(days_no_changes))}")
    else:
        lines.append("  Days with no upgrades: none")
    lines.append(SEP)
    return "\n".join(lines)
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

def extract_roster_units(pdata: dict) -> tuple:
    """Extract roster data from comlink player response.
    Returns (units, abilities) where abilities = {unit_id: [{id, tier, is_zeta}]}"""
    units = []
    abilities = {}
    STAR_MAP = {"ONE_STAR": 1, "TWO_STAR": 2, "THREE_STAR": 3, "FOUR_STAR": 4,
                "FIVE_STAR": 5, "SIX_STAR": 6, "SEVEN_STAR": 7}
    for unit in pdata.get("rosterUnit", []):
        def_id = unit.get("definitionId", "")
        parts = def_id.split(":") if ":" in def_id else [def_id, ""]
        unit_id = parts[0]
        if not unit_id:
            continue
        stars = unit.get("currentRarity") or unit.get("currentStar") or unit.get("currentStars") or STAR_MAP.get(parts[1], 1)
        relic_data = unit.get("relic", {})
        relic_tier = relic_data.get("currentTier", -1) if relic_data else -1
        units.append({
            "unit_id": unit_id,
            "level": unit.get("currentLevel", 1),
            "gear_tier": unit.get("currentTier", 1),
            "relic_tier": relic_tier if relic_tier is not None else -1,
            "stars": stars,
            "combat_type": unit.get("combatType", 1),
        })
        unit_abilities = []
        for skill in unit.get("skill", []):
            skill_id = skill.get("id", "")
            if skill_id:
                unit_abilities.append({
                    "id": skill_id,
                    "tier": skill.get("tier", 1),
                    "is_zeta": 1 if skill.get("isZeta", False) else 0,
                })
        if unit_abilities:
            abilities[unit_id] = unit_abilities
    return units, abilities

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
        units, abilities = extract_roster_units(pdata)
        if units:
            save_roster_snapshot(player_id, str(date.today()), units, abilities)
        return {"id": player_id, "name": name, "gp": total_gp}
    except Exception as e:
        print(f"  Error fetching {fallback_name}: {e}")
        return None

async def fetch_friends(is_final: bool = False):
    global collection_status
    print(f"[{date.today()}] Collecting friends data... (final={is_final})")
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
            save_snapshot("friends", players, is_final=is_final)
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
        save_snapshot(guild_id, players_with_gp, is_final=collection_status.get("is_final", False))
        print(f"  Saved {len(players_with_gp)} players for {guild_name}")

async def fetch_all(is_final: bool = False):
    global collection_status
    # Estimate total: 6 friends + 6 guilds x ~50 players
    estimated_total = len(FRIENDS) + len(GUILDS) * 50
    collection_status["running"] = True
    collection_status["done"] = 0
    collection_status["total"] = estimated_total
    collection_status["current"] = ""
    collection_status["is_final"] = is_final
    print(f"[{date.today()}] Starting full data collection... (final={is_final})")
    await fetch_friends(is_final=is_final)
    async with httpx.AsyncClient(timeout=120) as client:
        for guild in GUILDS:
            await fetch_guild(client, guild)
    collection_status["running"] = False
    collection_status["current"] = "Done"
    collection_status["done"] = collection_status["total"]
    set_setting("last_updated", datetime.now().strftime("%Y-%m-%d %H:%M"))
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
async def cron_trigger(request: Request, final: int = 0):
    token = request.headers.get("X-Cron-Token", "")
    cron_secret = os.getenv("CRON_SECRET", "")
    if not cron_secret or token != cron_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")
    is_final = final == 1
    asyncio.create_task(fetch_all(is_final=is_final))
    return {"status": "started", "is_final": is_final}

@app.get("/api/settings")
async def get_settings(auth: bool = Depends(check_auth)):
    return {
        "monthly_plan": int(get_setting("monthly_plan", "100000")),
        "last_updated": get_setting("last_updated", ""),
    }

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
    """Fetch unit names + thumbnails from swgoh-utils gamedata on GitHub and cache in DB."""
    LOC_URL = "https://raw.githubusercontent.com/swgoh-utils/gamedata/main/Loc_ENG_US.txt.json"
    UNITS_URL = "https://raw.githubusercontent.com/swgoh-utils/gamedata/main/units.json"
    print("Fetching unit names + thumbnails from swgoh-utils/gamedata GitHub...")

    known_ids = get_all_unit_ids()
    print(f"  Found {len(known_ids)} unique unit IDs in roster snapshots")

    async with httpx.AsyncClient(timeout=180) as client:
        loc_r, units_r = await asyncio.gather(
            client.get(LOC_URL),
            client.get(UNITS_URL),
        )
        loc_r.raise_for_status()
        units_r.raise_for_status()

        loc_map = loc_r.json().get("data", {})
        print(f"  Loaded {len(loc_map)} localization strings")

        # Build baseId → thumbnailName map from units.json
        units_data = units_r.json()
        unit_meta = {}  # baseId -> {thumbnailName, combatType}
        units_list = units_data if isinstance(units_data, list) else units_data.get("data", [])
        for u in units_list:
            base_id = u.get("baseId", "").upper()
            if base_id:
                unit_meta[base_id] = {
                    "thumbnailName": u.get("thumbnailName", ""),
                    "combatType": u.get("combatType", 1),
                }
        print(f"  Loaded {len(unit_meta)} unit metadata entries")

        names_to_save = {}
        for unit_id in known_ids:
            name = loc_map.get(f"UNIT_{unit_id}_NAME", "") or unit_id
            meta = unit_meta.get(unit_id, {})
            names_to_save[unit_id] = {
                "name": name,
                "combat_type": meta.get("combatType", 1),
                "thumbnail_name": meta.get("thumbnailName", ""),
            }

        # Also load ability names using same localization file
        ability_ids = get_all_ability_ids()
        print(f"  Found {len(ability_ids)} unique ability IDs")
        for ab_id in ability_ids:
            name = loc_map.get(f"{ab_id.upper()}_NAME", "") or ab_id
            names_to_save[ab_id.upper()] = {"name": name, "combat_type": 0, "thumbnail_name": ""}

        save_unit_names(names_to_save)
        print(f"  Cached {len(names_to_save)} unit + ability names")
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

@app.get("/api/friends/roster_month/{player_id}")
async def roster_month(player_id: str, month: str = None, auth: bool = Depends(check_auth)):
    if not month:
        from datetime import date
        month = date.today().strftime("%Y-%m")
    return get_roster_changes_for_month(player_id, month)

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

@app.get("/api/test/relic/{player_id}")
async def test_relic(player_id: str, auth: bool = Depends(check_auth)):
    """Show raw relic_tier values from DB for a player (for debugging)."""
    from database import get_conn
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT unit_id, relic_tier FROM roster_snapshots
            WHERE player_id = ? AND relic_tier >= 2
            ORDER BY relic_tier DESC LIMIT 20
        """, (player_id,)).fetchall()
    return [{"unit_id": r[0], "relic_tier_stored": r[1], "relic_tier_displayed": r[1] - 1} for r in rows]

@app.get("/api/test/unit_stat/{ally_code}")
async def test_unit_stat(ally_code: str, auth: bool = Depends(check_auth)):
    """Show raw stat fields from first rosterUnit for a player."""
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(f"{COMLINK_URL}/player", json={"payload": {"allyCode": ally_code}, "enums": False})
        r.raise_for_status()
        pdata = r.json()
        units = pdata.get("rosterUnit", [])
        if not units:
            return {"error": "no units"}
        u = units[0]
        return {
            "definitionId": u.get("definitionId"),
            "keys": list(u.keys()),
            "currentRarity": u.get("currentRarity"),
            "unitStat": u.get("unitStat"),
        }

@app.delete("/api/admin/roster_snapshot/{date}")
async def delete_roster_snapshot(date: str, auth: bool = Depends(check_auth)):
    """Delete all roster snapshots for a given date (one-time cleanup)."""
    from database import get_conn
    with get_conn() as conn:
        r1 = conn.execute("DELETE FROM roster_snapshots WHERE snapshot_date = ?", (date,))
        r2 = conn.execute("DELETE FROM roster_ability_snapshots WHERE snapshot_date = ?", (date,))
        conn.commit()
    return {"deleted_units": r1.rowcount, "deleted_abilities": r2.rowcount, "date": date}

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

@app.get("/api/friends/export/{player_id}")
async def export_player_txt(
    player_id: str,
    report_type: str = "month",
    month: str = None,
    year: str = None,
    auth: bool = Depends(check_auth),
):
    """Generate and download a TXT report for a friend.
    report_type=month&month=YYYY-MM  — report for one month
    report_type=year&year=YYYY       — report for entire year (all months)
    """
    import traceback
    from fastapi.responses import Response
    from datetime import date as date_cls

    try:
        # Find player name by playerId stored in DB (not allyCode)
        player_name = None
        from database import get_conn
        with get_conn() as conn:
            row = conn.execute(
                "SELECT player_name FROM snapshots WHERE guild_id='friends' AND player_id=? LIMIT 1",
                (player_id,)
            ).fetchone()
            if row:
                player_name = row[0]
        if not player_name:
            player_name = player_id

        if report_type == "month":
            if not month:
                month = date_cls.today().strftime("%Y-%m")
            y, m = int(month.split("-")[0]), int(month.split("-")[1])
            content = _generate_month_block(player_name, player_id, y, m) + "\n"
            filename = f"gp_{y}-{str(m).zfill(2)}.txt"
        else:
            if not year:
                year = str(date_cls.today().year)
            y = int(year)
            today = date_cls.today()
            blocks = []
            for m in range(1, 13):
                if y == today.year and m > today.month:
                    break
                blocks.append(_generate_month_block(player_name, player_id, y, m))
            content = "\n\n".join(blocks) + "\n"
            filename = f"gp_{y}.txt"

        return Response(
            content=content.encode("utf-8"),
            media_type="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        tb = traceback.format_exc()
        print(f"export_player_txt error:\n{tb}")
        raise HTTPException(status_code=500, detail=str(e))
