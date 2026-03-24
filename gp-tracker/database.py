import sqlite3
from datetime import date

DB_PATH = "/data/gp_tracker.db"

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
                UNIQUE(snapshot_date, guild_id, player_id)
            )
        """)
        conn.commit()

def save_snapshot(guild_id: str, players: list):
    today = str(date.today())
    with get_conn() as conn:
        for p in players:
            conn.execute("""
                INSERT OR REPLACE INTO snapshots (snapshot_date, guild_id, player_id, player_name, gp)
                VALUES (?, ?, ?, ?, ?)
            """, (today, guild_id, p["id"], p["name"], p["gp"]))
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
    from datetime import date
    today = str(date.today())
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
            players.append({
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

        players_raw.sort(key=lambda x: x["gp"], reverse=True)
        gp_ranks = {p["id"]: i+1 for i, p in enumerate(players_raw)}

        players = []
        for p in players_raw:
            streak = get_streak(guild_id, p["id"])
            activity = get_activity_level(guild_id, p["id"])
            rank = gp_ranks[p["id"]]
            rank_change = get_rank_change(guild_id, p["id"], rank)
            players.append({
                "name": p["name"],
                "gp": p["gp"],
                "gp_prev": p["gp_prev"],
                "diff": p["diff"],
                "diff_pct": p["diff_pct"],
                "streak": streak,
                "activity": activity,
                "rank": rank,
                "rank_change": rank_change,
            })

        players.sort(key=lambda x: x["diff"], reverse=True)

        return {
            "latest_date": last_date,
            "prev_date": first_date,
            "players": players,
            "dates": [first_date, last_date]
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
    """Count consecutive days of positive/negative GP growth."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT snapshot_date, gp FROM snapshots
            WHERE guild_id = ? AND player_id = ?
            ORDER BY snapshot_date DESC LIMIT 60
        """, (guild_id, player_id)).fetchall()

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

def _calc_level(gp_now: int, gp_30d: int) -> int:
    """Calculate activity level 1-10 based on 30-day GP growth."""
    import math
    growth = max(0, gp_now - gp_30d)
    return min(10, max(1, math.ceil(growth / 10000)))

def get_activity_level(guild_id: str, player_id: str) -> int:
    """
    Calculate activity level 1-10 using sliding 30-day window.
    Uses average of last 7 available daily levels for smoothing.
    Returns 1 (red) if insufficient data.
    """
    from datetime import datetime, timedelta

    with get_conn() as conn:
        # Get all snapshots for this player, newest first
        rows = conn.execute("""
            SELECT snapshot_date, gp FROM snapshots
            WHERE guild_id = ? AND player_id = ?
            ORDER BY snapshot_date DESC LIMIT 40
        """, (guild_id, player_id)).fetchall()

    if len(rows) < 2:
        return 1

    # Build date->gp map
    gp_map = {r[0]: r[1] for r in rows}
    dates_sorted = sorted(gp_map.keys())

    # For each of last 7 available dates, calculate level
    recent_dates = dates_sorted[-7:]
    levels = []

    for d in recent_dates:
        gp_now = gp_map[d]
        # Find nearest earlier date ~30 days ago
        target = (datetime.strptime(d, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        # Pick closest available date <= target
        earlier = [x for x in dates_sorted if x <= target]
        if not earlier:
            # Not enough history yet — use earliest available
            earliest = dates_sorted[0]
            if earliest == d:
                levels.append(1)
                continue
            gp_30d = gp_map[earliest]
        else:
            gp_30d = gp_map[earlier[-1]]

        levels.append(_calc_level(gp_now, gp_30d))

    if not levels:
        return 1

    import math
    return min(10, max(1, round(sum(levels) / len(levels))))

def get_rank_change(guild_id: str, player_id: str, current_diff_rank: int):
    """Compare GP Growth rank today vs yesterday."""
    with get_conn() as conn:
        dates = [r[0] for r in conn.execute("""
            SELECT DISTINCT snapshot_date FROM snapshots
            WHERE guild_id = ? ORDER BY snapshot_date DESC LIMIT 3
        """, (guild_id,)).fetchall()]

    if len(dates) < 2:
        return 0

    today = dates[0]
    yesterday = dates[1]

    with get_conn() as conn:
        today_gp = {r[0]: r[1] for r in conn.execute(
            "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, today)
        ).fetchall()}
        yesterday_gp = {r[0]: r[1] for r in conn.execute(
            "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
            (guild_id, yesterday)
        ).fetchall()}

    # Calculate yesterday's diffs (vs day before)
    with get_conn() as conn:
        if len(dates) >= 3:
            day_before = dates[2]
            day_before_gp = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE guild_id = ? AND snapshot_date = ?",
                (guild_id, day_before)
            ).fetchall()}
        else:
            day_before_gp = yesterday_gp

    yesterday_diffs = []
    for pid, gp in yesterday_gp.items():
        gp_prev = day_before_gp.get(pid, gp)
        yesterday_diffs.append((pid, gp - gp_prev))

    yesterday_diffs.sort(key=lambda x: x[1], reverse=True)
    prev_rank_map = {pid: i+1 for i, (pid, _) in enumerate(yesterday_diffs)}

    prev_rank = prev_rank_map.get(player_id)
    if prev_rank is None:
        return 0
    return prev_rank - current_diff_rank  # positive = moved up

def get_monthly_achievements(guild_id: str) -> dict:
    """Calculate achievements for each player based on last completed month and current streak."""
    from datetime import date, timedelta
    from calendar import monthrange

    today = date.today()
    # Last month
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    with get_conn() as conn:
        # Get last month's first and last snapshots
        lm_first = conn.execute("""
            SELECT MIN(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date >= ? AND snapshot_date <= ?
        """, (guild_id, str(last_month_start), str(last_month_end))).fetchone()[0]

        lm_last = conn.execute("""
            SELECT MAX(snapshot_date) FROM snapshots
            WHERE guild_id = ? AND snapshot_date >= ? AND snapshot_date <= ?
        """, (guild_id, str(last_month_start), str(last_month_end))).fetchone()[0]

    achievements = {}  # player_name -> list of (emoji, tooltip)

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
        month_name = last_month_end.strftime("%B %Y")

        for i, (pid, name, diff) in enumerate(diffs):
            if name not in achievements:
                achievements[name] = []
            rank = i + 1
            if diff < 0:
                achievements[name].append(("☠️", f"Від'ємний приріст GP у {month_name}"))
            elif diff == 0:
                achievements[name].append(("❄️", f"Нульовий приріст у {month_name}"))
            elif rank == 1:
                achievements[name].append(("🥇", f"1-е місце по приросту GP у {month_name}"))
            elif rank == 2:
                achievements[name].append(("🥈", f"2-е місце по приросту GP у {month_name}"))
            elif rank == 3:
                achievements[name].append(("🥉", f"3-є місце по приросту GP у {month_name}"))
            elif rank == len(diffs):
                achievements[name].append(("🐢", f"Останнє місце по приросту GP у {month_name}"))

    # Streak achievements (current)
    with get_conn() as conn:
        player_ids = [r[0] for r in conn.execute(
            "SELECT DISTINCT player_id FROM snapshots WHERE guild_id = ? ORDER BY snapshot_date DESC LIMIT 1000",
            (guild_id,)
        ).fetchall()]
        names = {r[0]: r[1] for r in conn.execute(
            "SELECT DISTINCT player_id, player_name FROM snapshots WHERE guild_id = ?",
            (guild_id,)
        ).fetchall()}

    for pid in player_ids:
        streak = get_streak(guild_id, pid)
        name = names.get(pid)
        if not name:
            continue
        if name not in achievements:
            achievements[name] = []

        if streak >= 30:
            achievements[name].append(("🔥🔥🔥", f"Streak {streak} днів поспіль — весь місяць!"))
        elif streak >= 14:
            achievements[name].append(("🔥🔥", f"Streak {streak} днів поспіль"))
        elif streak >= 7:
            achievements[name].append(("🔥", f"Streak {streak} днів поспіль"))
        elif streak <= -6:
            achievements[name].append(("🐌", f"Не качається {abs(streak)} днів поспіль"))
        elif streak <= -3:
            achievements[name].append(("🥶", f"Заморожений — {abs(streak)} дні без приросту"))

    return achievements

def get_friends_history(player_ids: list):
    with get_conn() as conn:
        placeholders = ",".join("?" * len(player_ids))
        rows = conn.execute(f"""
            SELECT snapshot_date, player_id, player_name, gp
            FROM snapshots
            WHERE guild_id = 'friends' AND player_id IN ({placeholders})
            ORDER BY snapshot_date ASC
        """, player_ids).fetchall()

        players = {}
        for snapshot_date, player_id, player_name, gp in rows:
            if player_id not in players:
                players[player_id] = {"name": player_name, "history": []}
            players[player_id]["history"].append({"date": snapshot_date, "gp": gp})

        return list(players.values())
