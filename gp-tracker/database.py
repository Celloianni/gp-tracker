import sqlite3
from datetime import date, datetime
from zoneinfo import ZoneInfo

DB_PATH = "/data/gp_tracker.db"
_KYIV = ZoneInfo("Europe/Kyiv")

def today_kyiv() -> str:
    """Return today's date string in Kyiv timezone (YYYY-MM-DD)."""
    return datetime.now(_KYIV).strftime("%Y-%m-%d")

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                guild_id TEXT NOT NULL,
                player_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                gp INTEGER NOT NULL,
                is_final INTEGER NOT NULL DEFAULT 0,
                UNIQUE(snapshot_date, guild_id, player_id)
            )
        """)
        try:
            conn.execute("ALTER TABLE snapshots ADD COLUMN is_final INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roster_snapshots (
                snapshot_date TEXT NOT NULL,
                player_id TEXT NOT NULL,
                unit_id TEXT NOT NULL,
                current_level INTEGER NOT NULL DEFAULT 1,
                gear_tier INTEGER NOT NULL DEFAULT 1,
                relic_tier INTEGER NOT NULL DEFAULT -1,
                current_stars INTEGER NOT NULL DEFAULT 1,
                combat_type INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (snapshot_date, player_id, unit_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS unit_names (
                unit_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                combat_type INTEGER NOT NULL DEFAULT 1,
                thumbnail_name TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE unit_names ADD COLUMN thumbnail_name TEXT")
        except Exception:
            pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS roster_ability_snapshots (
                snapshot_date TEXT NOT NULL,
                player_id TEXT NOT NULL,
                unit_id TEXT NOT NULL,
                ability_id TEXT NOT NULL,
                tier INTEGER NOT NULL DEFAULT 1,
                is_zeta INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (snapshot_date, player_id, unit_id, ability_id)
            )
        """)
        conn.commit()

def save_snapshot(guild_id: str, players: list, is_final: bool = False):
    today = today_kyiv()
    with get_conn() as conn:
        for p in players:
            conn.execute("""
                INSERT INTO snapshots (snapshot_date, guild_id, player_id, player_name, gp, is_final)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_date, guild_id, player_id) DO UPDATE SET
                    player_name = excluded.player_name,
                    gp = excluded.gp,
                    is_final = CASE WHEN excluded.is_final = 1 THEN 1 ELSE is_final END
            """, (today, guild_id, p["id"], p["name"], p["gp"], 1 if is_final else 0))
        conn.commit()

def is_empty():
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()
        return row[0] == 0

def get_progress(guild_id: str):
    with get_conn() as conn:
        dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT snapshot_date FROM snapshots WHERE guild_id = ? ORDER BY snapshot_date DESC LIMIT 30",
            (guild_id,)
        ).fetchall()]

        if not dates:
            return {"dates": [], "players": [], "latest_date": None, "prev_date": None}

        latest_date = dates[0]
        prev_date = dates[1] if len(dates) > 1 else None

        latest = {r[0]: {"name": r[1], "gp": r[2]} for r in conn.execute(
            "SELECT player_id, player_name, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, latest_date)
        ).fetchall()}

        prev = {}
        if prev_date:
            prev = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                (guild_id, prev_date)
            ).fetchall()}

        # Build players with GP rank
        players_raw = []
        for pid, data in latest.items():
            gp_now = data["gp"]
            gp_prev = prev.get(pid, gp_now)
            diff = gp_now - gp_prev
            players_raw.append({
                "id": pid,
                "name": data["name"],
                "gp": gp_now,
                "gp_prev": gp_prev,
                "diff": diff,
                "diff_pct": round(diff / gp_prev * 100, 2) if gp_prev > 0 else 0
            })

        # Sort by GP for rank assignment
        players_raw.sort(key=lambda x: x["gp"], reverse=True)
        gp_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        # Sort by diff to get diff ranks
        players_raw.sort(key=lambda x: x["diff"], reverse=True)
        diff_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        players = []
        for p in players_raw:
            streak = get_streak(guild_id, p["id"])
            activity = get_activity_level(guild_id, p["id"])
            diff_rank = diff_ranks[p["id"]]
            rank_change = get_rank_change(guild_id, p["id"], diff_rank)
            players.append({
                "name": p["name"],
                "gp": p["gp"],
                "gp_prev": p["gp_prev"],
                "diff": p["diff"],
                "diff_pct": p["diff_pct"],
                "streak": streak,
                "activity": activity,
                "rank": diff_rank,
                "rank_change": rank_change,
            })

        return {
            "latest_date": latest_date,
            "prev_date": prev_date,
            "players": players,
            "dates": dates
        }

def get_monthly_progress(guild_id: str):
    """Get GP growth from 1st of current month to today for all players."""
    from datetime import timedelta
    today = today_kyiv()
    week_ago = str((datetime.now(_KYIV) - timedelta(days=7)).date())
    month_start = today[:8] + "01"

    with get_conn() as conn:
        # First snapshot of this month
        first_date = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date >= ?
        """, (guild_id, month_start)).fetchone()[0]

        # Latest snapshot
        last_date = conn.execute("""
            SELECT MAX(snapshot_date) FROM snapshots
            WHERE guild_id = ?
        """, (guild_id,)).fetchone()[0]

        if not first_date or not last_date:
            return {"dates": [], "players": [], "latest_date": None, "prev_date": None}

        latest = {r[0]: {"name": r[1], "gp": r[2]} for r in conn.execute(
            "SELECT player_id, player_name, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, last_date)
        ).fetchall()}

        prev = {}
        if first_date != last_date:
            prev = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                (guild_id, first_date)
            ).fetchall()}

        # First ever snapshot for this guild (to distinguish truly new members)
        guild_first_date = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots WHERE guild_id = ?
        """, (guild_id,)).fetchone()[0]

        # Join dates: first ever snapshot per player in this guild
        join_dates = {r[0]: r[1] for r in conn.execute("""
            SELECT player_id, MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? GROUP BY player_id
        """, (guild_id,)).fetchall()}

        # Previous snapshot (for daily diff — change since last cron)
        prev_snapshot_date = conn.execute("""
            SELECT MAX(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date < ?
        """, (guild_id, last_date)).fetchone()[0]
        prev_snapshot_gp = {}
        if prev_snapshot_date:
            prev_snapshot_gp = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                (guild_id, prev_snapshot_date)
            ).fetchall()}

        monthly_plan = int(get_setting("monthly_plan", "100000"))

        players_raw = []
        for pid, data in latest.items():
            gp_now = data["gp"]
            gp_prev = prev.get(pid, gp_now)
            diff = gp_now - gp_prev
            daily_diff = gp_now - prev_snapshot_gp.get(pid, gp_now)
            players_raw.append({
                "id": pid,
                "name": data["name"],
                "gp": gp_now,
                "gp_prev": gp_prev,
                "diff": diff,
                "daily_diff": daily_diff,
                "diff_pct": round(diff / gp_prev * 100, 2) if gp_prev > 0 else 0,
                "plan_pct": round(diff / monthly_plan * 100, 1) if monthly_plan > 0 else 0,
                "join_date": join_dates.get(pid),
            })

        players_raw.sort(key=lambda x: x["gp"], reverse=True)
        gp_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        # Sort by diff to get diff ranks
        players_raw.sort(key=lambda x: x["diff"], reverse=True)
        diff_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        players = []
        for p in players_raw:
            streak = get_streak(guild_id, p["id"])
            activity = get_activity_level(guild_id, p["id"])
            diff_rank = diff_ranks[p["id"]]
            rank_change = get_rank_change(guild_id, p["id"], diff_rank)
            jd = p["join_date"]
            players.append({
                "id": p["id"],
                "name": p["name"],
                "gp": p["gp"],
                "gp_prev": p["gp_prev"],
                "diff": p["diff"],
                "daily_diff": p["daily_diff"],
                "diff_pct": p["diff_pct"],
                "plan_pct": p["plan_pct"],
                "streak": streak,
                "activity": activity,
                "rank": diff_rank,
                "rank_change": rank_change,
                "join_date": jd,
                "is_new": bool(jd and jd >= week_ago and jd > guild_first_date),
            })

        return {
            "latest_date": last_date,
            "prev_date": first_date,
            "players": players,
            "dates": [first_date, last_date],
            "monthly_plan": monthly_plan,
        }

def get_available_months(guild_id: str):
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT substr(snapshot_date, 1, 7) as month
            FROM snapshots WHERE guild_id = ?
            ORDER BY month DESC
        """, (guild_id,)).fetchall()
        return [r[0] for r in rows]

def get_progress_for_month(guild_id: str, month: str):
    from datetime import timedelta
    today = today_kyiv()
    week_ago = str((datetime.now(_KYIV) - timedelta(days=7)).date())
    with get_conn() as conn:
        # First snapshot of the month
        first_date = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND substr(snapshot_date, 1, 7) = ?
        """, (guild_id, month)).fetchone()[0]

        # Last snapshot of the month
        last_date = conn.execute("""
            SELECT MAX(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND substr(snapshot_date, 1, 7) = ?
        """, (guild_id, month)).fetchone()[0]

        if not first_date or not last_date:
            return {"dates": [], "players": [], "latest_date": None, "prev_date": None}

        # If same date — only one snapshot, diff = 0
        latest = {r[0]: {"name": r[1], "gp": r[2]} for r in conn.execute(
            "SELECT player_id, player_name, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, last_date)
        ).fetchall()}

        prev = {}
        if first_date != last_date:
            prev = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                (guild_id, first_date)
            ).fetchall()}

        # First ever snapshot for this guild (to distinguish truly new members)
        guild_first_date = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots WHERE guild_id = ?
        """, (guild_id,)).fetchone()[0]

        # Join dates: first ever snapshot per player in this guild
        join_dates = {r[0]: r[1] for r in conn.execute("""
            SELECT player_id, MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? GROUP BY player_id
        """, (guild_id,)).fetchall()}

        monthly_plan = int(get_setting("monthly_plan", "100000"))

        players_raw = []
        for pid, data in latest.items():
            gp_now = data["gp"]
            gp_prev = prev.get(pid, gp_now)
            diff = gp_now - gp_prev
            players_raw.append({
                "id": pid,
                "name": data["name"],
                "gp": gp_now,
                "gp_prev": gp_prev,
                "diff": diff,
                "diff_pct": round(diff / gp_prev * 100, 2) if gp_prev > 0 else 0,
                "plan_pct": round(diff / monthly_plan * 100, 1) if monthly_plan > 0 else 0,
                "join_date": join_dates.get(pid),
            })

        players_raw.sort(key=lambda x: x["diff"], reverse=True)
        diff_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        players = []
        for p in players_raw:
            streak = get_streak(guild_id, p["id"])
            activity = get_activity_level(guild_id, p["id"])
            diff_rank = diff_ranks[p["id"]]
            rank_change = get_rank_change(guild_id, p["id"], diff_rank)
            jd = p["join_date"]
            players.append({
                "id": p["id"],
                "name": p["name"],
                "gp": p["gp"],
                "gp_prev": p["gp_prev"],
                "diff": p["diff"],
                "diff_pct": p["diff_pct"],
                "plan_pct": p["plan_pct"],
                "streak": streak,
                "activity": activity,
                "rank": diff_rank,
                "rank_change": rank_change,
                "join_date": jd,
                "is_new": bool(jd and jd >= week_ago and jd > guild_first_date),
            })

        players.sort(key=lambda x: x["diff"], reverse=True)

        return {
            "latest_date": last_date,
            "prev_date": first_date,
            "players": players,
            "dates": [first_date, last_date],
            "monthly_plan": monthly_plan,
        }

def get_setting(key: str, default: str = "") -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

def set_setting(key: str, value: str):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()

def get_streak(guild_id: str, player_id: str):
    """Count consecutive days of positive/negative GP growth.
    Uses only is_final=1 snapshots (00:00 updates) when available,
    falls back to all snapshots for older data.
    Today's non-final snapshot is always excluded to avoid breaking
    streaks before the day is complete."""
    today = today_kyiv()
    with get_conn() as conn:
        # Try final snapshots first
        rows = conn.execute("""
            SELECT snapshot_date, gp FROM snapshots
            WHERE guild_id = ? AND player_id = ? AND is_final = 1
            ORDER BY snapshot_date DESC LIMIT 60
        """, (guild_id, player_id)).fetchall()
        # Fall back to all snapshots if not enough finals,
        # but exclude today's snapshot if it's not final yet
        if len(rows) < 2:
            rows = conn.execute("""
                SELECT snapshot_date, gp FROM snapshots
                WHERE guild_id = ? AND player_id = ?
                AND NOT (snapshot_date = ? AND is_final = 0)
                ORDER BY snapshot_date DESC LIMIT 60
            """, (guild_id, player_id, today)).fetchall()

    if len(rows) < 2:
        return 0

    diffs = []
    for i in range(len(rows) - 1):
        diffs.append(rows[i][1] - rows[i+1][1])

    if not diffs:
        return 0

    first_positive = diffs[0] > 0
    streak = 0
    for d in diffs:
        if (d > 0) == first_positive:
            streak += 1
        else:
            break

    return streak if first_positive else -streak

def get_activity_level(guild_id: str, player_id: str) -> int:
    """
    Calculate activity level 1-10 based on smoothed streak history.
    Each day of positive GP growth: +1 level (max 10)
    Each day of negative/zero GP growth: -1 level (min 1)
    Starts at level 1 if no data.
    Uses is_final=1 snapshots when available, falls back to all snapshots.
    """
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT snapshot_date, gp FROM snapshots
            WHERE guild_id = ? AND player_id = ? AND is_final = 1
            ORDER BY snapshot_date ASC
        """, (guild_id, player_id)).fetchall()
        if len(rows) < 2:
            rows = conn.execute("""
                SELECT snapshot_date, gp FROM snapshots
                WHERE guild_id = ? AND player_id = ?
                ORDER BY snapshot_date ASC
            """, (guild_id, player_id)).fetchall()

    if len(rows) < 2:
        return 1

    level = 1
    for i in range(1, len(rows)):
        diff = rows[i][1] - rows[i-1][1]
        if diff > 0:
            level = min(10, level + 1)
        else:
            level = max(1, level - 1)

    return level

def get_rank_change(guild_id: str, player_id: str, current_diff_rank: int):
    """Compare GP Growth rank today vs yesterday, both measured from start of month."""
    today = today_kyiv()
    month_start = today[:8] + "01"

    with get_conn() as conn:
        # Yesterday = most recent snapshot before today
        row = conn.execute("""
            SELECT MAX(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date < ?
        """, (guild_id, today)).fetchone()
        yesterday = row[0] if row else None

        if not yesterday:
            return 0

        # Start of month snapshot
        first_date = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date >= ?
        """, (guild_id, month_start)).fetchone()[0]

        if not first_date or first_date == yesterday:
            return 0

        yesterday_gp = {r[0]: r[1] for r in conn.execute(
            "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, yesterday)
        ).fetchall()}

        month_start_gp = {r[0]: r[1] for r in conn.execute(
            "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, first_date)
        ).fetchall()}

    # Yesterday's rank = monthly growth up to yesterday
    yesterday_diffs = []
    for pid, gp in yesterday_gp.items():
        gp_prev = month_start_gp.get(pid, gp)
        yesterday_diffs.append((pid, gp - gp_prev))

    yesterday_diffs.sort(key=lambda x: x[1], reverse=True)
    prev_rank_map = {pid: i+1 for i, (pid, _) in enumerate(yesterday_diffs)}

    prev_rank = prev_rank_map.get(player_id)
    if prev_rank is None:
        return 0
    return prev_rank - current_diff_rank  # positive = moved up

def get_monthly_achievements(guild_id: str) -> dict:
    """Calculate achievements. Monthly ones shown only first 7 days of new month."""
    from datetime import timedelta

    today = datetime.now(_KYIV).date()
    achievements = {}  # player_name -> list of (emoji, tooltip)

    # Monthly achievements — only show first 7 days of the month
    if today.day <= 7:
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        ua_months = {
            1: "Січні", 2: "Лютому", 3: "Березні", 4: "Квітні",
            5: "Травні", 6: "Червні", 7: "Липні", 8: "Серпні",
            9: "Вересні", 10: "Жовтні", 11: "Листопаді", 12: "Грудні"
        }
        month_name = f"у {ua_months[last_month_end.month]} {last_month_end.year}"

        with get_conn() as conn:
            lm_first = conn.execute("""
                SELECT MIN(snapshot_date) FROM snapshots
                WHERE guild_id = ? AND snapshot_date >= ? AND snapshot_date <= ?
            """, (guild_id, str(last_month_start), str(last_month_end))).fetchone()[0]

            lm_last = conn.execute("""
                SELECT MAX(snapshot_date) FROM snapshots
                WHERE guild_id = ? AND snapshot_date >= ? AND snapshot_date <= ?
            """, (guild_id, str(last_month_start), str(last_month_end))).fetchone()[0]

        if lm_first and lm_last and lm_first != lm_last:
            with get_conn() as conn:
                latest_gp = {r[0]: (r[1], r[2]) for r in conn.execute(
                    "SELECT player_id, player_name, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                    (guild_id, lm_last)
                ).fetchall()}
                first_gp = {r[0]: r[1] for r in conn.execute(
                    "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                    (guild_id, lm_first)
                ).fetchall()}

            diffs = []
            for pid, (name, gp_now) in latest_gp.items():
                gp_prev = first_gp.get(pid, gp_now)
                diffs.append((pid, name, gp_now - gp_prev))

            diffs.sort(key=lambda x: x[2], reverse=True)

            for i, (pid, name, diff) in enumerate(diffs):
                if name not in achievements:
                    achievements[name] = []
                rank = i + 1
                if diff < 0:
                    achievements[name].append(("☠️", f"Від'ємний приріст GP у {month_name}"))
                elif diff == 0:
                    achievements[name].append(("❄️", f"Нульовий приріст у {month_name}"))
                elif rank == 1:
                    achievements[name].append(("🥇", f"1-й по приросту GP у {month_name}"))
                elif rank == 2:
                    achievements[name].append(("🥈", f"2-й по приросту GP у {month_name}"))
                elif rank == 3:
                    achievements[name].append(("🥉", f"3-й по приросту GP у {month_name}"))
                elif rank == len(diffs):
                    achievements[name].append(("🐌", f"Равлик по приросту GP у {month_name}"))

    return achievements

def get_friends_history():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT snapshot_date, player_id, player_name, gp
            FROM snapshots
            WHERE guild_id = 'friends'
            ORDER BY snapshot_date ASC
        """).fetchall()

        players = {}
        for snapshot_date, player_id, player_name, gp in rows:
            if player_id not in players:
                players[player_id] = {"name": player_name, "history": []}
            players[player_id]["history"].append({"date": snapshot_date, "gp": gp})

        return list(players.values())

def save_roster_snapshot(player_id: str, snapshot_date: str, units: list, abilities: dict = None):
    """Save roster snapshot. abilities = {unit_id: [{id, tier, is_zeta}, ...]}"""
    with get_conn() as conn:
        for u in units:
            conn.execute("""
                INSERT OR REPLACE INTO roster_snapshots
                (snapshot_date, player_id, unit_id, current_level, gear_tier, relic_tier, current_stars, combat_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (snapshot_date, player_id, u["unit_id"], u["level"], u["gear_tier"], u["relic_tier"], u["stars"], u["combat_type"]))
        if abilities:
            for unit_id, unit_abilities in abilities.items():
                for a in unit_abilities:
                    conn.execute("""
                        INSERT OR REPLACE INTO roster_ability_snapshots
                        (snapshot_date, player_id, unit_id, ability_id, tier, is_zeta)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (snapshot_date, player_id, unit_id, a["id"], a["tier"], a["is_zeta"]))
        conn.commit()

def save_unit_names(units: dict):
    """units = {unit_id: {"name": "Darth Revan", "combat_type": 1, "thumbnail_name": "tex.charui_sithrevan"}}"""
    with get_conn() as conn:
        for unit_id, data in units.items():
            conn.execute("""
                INSERT OR REPLACE INTO unit_names (unit_id, name, combat_type, thumbnail_name)
                VALUES (?, ?, ?, ?)
            """, (unit_id, data["name"], data.get("combat_type", 1), data.get("thumbnail_name")))
        conn.commit()

def get_unit_names_count() -> int:
    with get_conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM unit_names").fetchone()[0]

def get_all_unit_ids() -> list:
    """Get all unique unit IDs stored in roster_snapshots."""
    with get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT unit_id FROM roster_snapshots").fetchall()
        return [r[0] for r in rows]

def get_all_ability_ids() -> list:
    """Get all unique ability IDs stored in roster_ability_snapshots."""
    with get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT ability_id FROM roster_ability_snapshots").fetchall()
        return [r[0] for r in rows]

def get_roster_dates(player_id: str) -> list:
    """Get all dates with roster data for a player."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT DISTINCT snapshot_date FROM roster_snapshots
            WHERE player_id = ?
            ORDER BY snapshot_date DESC
        """, (player_id,)).fetchall()
        return [r[0] for r in rows]

def get_roster_changes(player_id: str, date: str = None) -> dict:
    """Compare roster on given date vs previous date. Returns list of changes."""
    with get_conn() as conn:
        if date:
            dates = [r[0] for r in conn.execute("""
                SELECT DISTINCT snapshot_date FROM roster_snapshots
                WHERE player_id = ? AND snapshot_date <= ?
                ORDER BY snapshot_date DESC LIMIT 2
            """, (player_id, date)).fetchall()]
        else:
            dates = [r[0] for r in conn.execute("""
                SELECT DISTINCT snapshot_date FROM roster_snapshots
                WHERE player_id = ?
                ORDER BY snapshot_date DESC LIMIT 2
            """, (player_id,)).fetchall()]

        if not dates:
            return {"date": None, "prev_date": None, "changes": []}

        today = dates[0]
        prev_date = dates[1] if len(dates) > 1 else None

        if not prev_date:
            return {"date": today, "prev_date": None, "changes": []}

        today_roster = {r[0]: {"level": r[1], "gear_tier": r[2], "relic_tier": r[3], "stars": r[4], "combat_type": r[5]}
                        for r in conn.execute("""
                            SELECT unit_id, current_level, gear_tier, relic_tier, current_stars, combat_type
                            FROM roster_snapshots WHERE player_id = ? AND snapshot_date = ?
                        """, (player_id, today)).fetchall()}

        prev_roster = {}
        if prev_date:
            prev_roster = {r[0]: {"level": r[1], "gear_tier": r[2], "relic_tier": r[3], "stars": r[4], "combat_type": r[5]}
                           for r in conn.execute("""
                               SELECT unit_id, current_level, gear_tier, relic_tier, current_stars, combat_type
                               FROM roster_snapshots WHERE player_id = ? AND snapshot_date = ?
                           """, (player_id, prev_date)).fetchall()}

        unit_data = {r[0]: {"name": r[1], "thumbnail_name": r[2], "combat_type": r[3]} for r in conn.execute("SELECT unit_id, name, thumbnail_name, combat_type FROM unit_names").fetchall()}
        unit_names = {uid: d["name"] for uid, d in unit_data.items()}

        # Load abilities for both dates
        today_abilities = {}
        for r in conn.execute("""
            SELECT unit_id, ability_id, tier, is_zeta FROM roster_ability_snapshots
            WHERE player_id = ? AND snapshot_date = ?
        """, (player_id, today)).fetchall():
            today_abilities.setdefault(r[0], {})[r[1]] = {"tier": r[2], "is_zeta": r[3]}

        prev_abilities = {}
        if prev_date:
            for r in conn.execute("""
                SELECT unit_id, ability_id, tier, is_zeta FROM roster_ability_snapshots
                WHERE player_id = ? AND snapshot_date = ?
            """, (player_id, prev_date)).fetchall():
                prev_abilities.setdefault(r[0], {})[r[1]] = {"tier": r[2], "is_zeta": r[3]}

        changes = []
        for unit_id, current in today_roster.items():
            name = unit_names.get(unit_id, unit_id)
            ud = unit_data.get(unit_id, {})
            thumb = ud.get("thumbnail_name") or ""
            thumbnail_url = f"https://game-assets.swgoh.gg/textures/{thumb}.png" if thumb else ""
            combat_type = ud.get("combat_type") or current["combat_type"]
            if unit_id not in prev_roster:
                changes.append({
                    "type": "new",
                    "unit_id": unit_id,
                    "name": name,
                    "thumbnail_url": thumbnail_url,
                    "stars": current["stars"],
                    "level": current["level"],
                    "gear_tier": current["gear_tier"],
                    "relic_tier": current["relic_tier"],
                    "combat_type": combat_type,
                })
            else:
                prev = prev_roster[unit_id]
                unit_changes = []
                if current["stars"] > prev["stars"]:
                    unit_changes.append({"field": "stars", "from": prev["stars"], "to": current["stars"]})
                if current["level"] > prev["level"]:
                    unit_changes.append({"field": "level", "from": prev["level"], "to": current["level"]})
                if current["gear_tier"] > prev["gear_tier"]:
                    unit_changes.append({"field": "gear_tier", "from": prev["gear_tier"], "to": current["gear_tier"]})
                if current["relic_tier"] > prev["relic_tier"] and current["relic_tier"] >= 2:
                    unit_changes.append({"field": "relic_tier", "from": prev["relic_tier"], "to": current["relic_tier"]})
                # Compare abilities
                cur_abs = today_abilities.get(unit_id, {})
                prv_abs = prev_abilities.get(unit_id, {})
                for ab_id, ab_cur in cur_abs.items():
                    ab_prv = prv_abs.get(ab_id)
                    ab_name = unit_names.get(ab_id.upper(), ab_id)
                    if ab_prv is None:
                        unit_changes.append({"field": "ability_new", "ability_id": ab_id, "ability_name": ab_name, "tier": ab_cur["tier"], "is_zeta": ab_cur["is_zeta"]})
                    elif ab_cur["tier"] > ab_prv["tier"] or (ab_cur["is_zeta"] and not ab_prv["is_zeta"]):
                        unit_changes.append({"field": "ability", "ability_id": ab_id, "ability_name": ab_name,
                                             "from": ab_prv["tier"], "to": ab_cur["tier"], "is_zeta": ab_cur["is_zeta"]})
                if unit_changes:
                    changes.append({
                        "type": "upgrade",
                        "unit_id": unit_id,
                        "name": name,
                        "thumbnail_url": thumbnail_url,
                        "combat_type": combat_type,
                        "changes": unit_changes,
                    })

        # Sort: new units first, then upgrades; within each group by name
        changes.sort(key=lambda x: (0 if x["type"] == "new" else 1, x["name"]))

        return {
            "date": today,
            "prev_date": prev_date,
            "changes": changes,
        }

def get_player_gp_for_period(player_id: str, date_from: str, date_to: str) -> dict:
    """Get first and last GP for a player in a date range (guild_id='friends')."""
    with get_conn() as conn:
        first = conn.execute("""
            SELECT gp, snapshot_date FROM snapshots WHERE guild_id = 'friends' AND player_id = ?
            AND snapshot_date >= ? AND snapshot_date <= ?
            ORDER BY snapshot_date ASC LIMIT 1
        """, (player_id, date_from, date_to)).fetchone()
        last = conn.execute("""
            SELECT gp, snapshot_date FROM snapshots WHERE guild_id = 'friends' AND player_id = ?
            AND snapshot_date >= ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC LIMIT 1
        """, (player_id, date_from, date_to)).fetchone()
    return {
        "gp_start": first[0] if first else None,
        "gp_start_date": first[1] if first else None,
        "gp_end": last[0] if last else None,
        "gp_end_date": last[1] if last else None,
    }


def get_roster_changes_for_month(player_id: str, year_month: str) -> dict:
    """Get roster changes for each day in a month. Returns {date: {has_snapshot, has_prev, changes}}"""
    with get_conn() as conn:
        all_dates = [r[0] for r in conn.execute("""
            SELECT DISTINCT snapshot_date FROM roster_snapshots
            WHERE player_id = ?
            ORDER BY snapshot_date
        """, (player_id,)).fetchall()]

    month_dates = [d for d in all_dates if d.startswith(year_month)]

    result = {}
    for date_str in month_dates:
        prev_dates = [d for d in all_dates if d < date_str]
        prev_date = prev_dates[-1] if prev_dates else None
        if prev_date:
            changes_data = get_roster_changes(player_id, date_str)
            result[date_str] = {
                "has_snapshot": True,
                "has_prev": True,
                "changes": changes_data["changes"]
            }
        else:
            result[date_str] = {
                "has_snapshot": True,
                "has_prev": False,
                "changes": []
            }
    return result
