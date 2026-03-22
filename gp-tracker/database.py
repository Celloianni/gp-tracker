import sqlite3
from datetime import date, timedelta

DB_PATH = "/data/gp_tracker.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                player_id TEXT NOT NULL,
                player_name TEXT NOT NULL,
                gp INTEGER NOT NULL,
                UNIQUE(snapshot_date, player_id)
            )
        """)
        conn.commit()

def save_snapshot(players: list):
    today = str(date.today())
    with get_conn() as conn:
        for p in players:
            conn.execute("""
                INSERT OR REPLACE INTO snapshots (snapshot_date, player_id, player_name, gp)
                VALUES (?, ?, ?, ?)
            """, (today, p["id"], p["name"], p["gp"]))
        conn.commit()

def is_empty():
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()
        return row[0] == 0

def get_progress():
    with get_conn() as conn:
        # Берём все доступные даты
        dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT snapshot_date FROM snapshots ORDER BY snapshot_date DESC LIMIT 30"
        ).fetchall()]

        if not dates:
            return {"dates": [], "players": []}

        latest_date = dates[0]
        prev_date = dates[1] if len(dates) > 1 else None

        # Данные за последний день
        latest = {r[0]: {"name": r[1], "gp": r[2]} for r in conn.execute(
            "SELECT player_id, player_name, gp FROM snapshots WHERE snapshot_date = ?",
            (latest_date,)
        ).fetchall()}

        # Данные за предыдущий день
        prev = {}
        if prev_date:
            prev = {r[0]: r[1] for r in conn.execute(
                "SELECT player_id, gp FROM snapshots WHERE snapshot_date = ?",
                (prev_date,)
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

        # Сортируем по приросту (больший прирост — выше)
        players.sort(key=lambda x: x["diff"], reverse=True)
        for i, p in enumerate(players):
            p["rank"] = i + 1

        return {
            "latest_date": latest_date,
            "prev_date": prev_date,
            "players": players,
            "dates": dates
        }
