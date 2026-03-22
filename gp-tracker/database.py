import sqlite3
from datetime import date

DB_PATH = "/data/gp_tracker.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    with get_conn() as conn:
        conn.execute("DROP TABLE IF EXISTS snapshots")
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

        players = []
        for pid, data in latest.items():
            gp_now = data["gp"]
            gp_prev = prev.get(pid, gp_now)
            diff = gp_now - gp_prev
            players.append({
                "name": data["name"],
                "gp": gp_now,
                "gp_prev": gp_prev,
                "diff": diff,
                "diff_pct": round(diff / gp_prev * 100, 2) if gp_prev > 0 else 0
            })

        players.sort(key=lambda x: x["diff"], reverse=True)
        for i, p in enumerate(players):
            p["rank"] = i + 1

        return {
            "latest_date": latest_date,
            "prev_date": prev_date,
            "players": players,
            "dates": dates
        }

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
