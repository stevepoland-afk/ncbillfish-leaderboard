"""
NC Billfish Series Leaderboard — Backend API
FastAPI + SQLite backend for multi-tenant fishing tournament leaderboards.
"""

import os
import json
import sqlite3
import secrets
from datetime import datetime, timedelta
from typing import Optional, List
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Query, Depends, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from jose import jwt, JWTError
from passlib.context import CryptContext

# ============================================================================
# Configuration
# ============================================================================

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.environ.get("DATABASE_PATH", os.path.join(_THIS_DIR, "leaderboard.db"))
JWT_SECRET = os.environ.get("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = 24
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", os.path.join(_THIS_DIR, "uploads"))
PARENT_DIR = os.path.dirname(_THIS_DIR)
DEFAULT_ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@ncbillfish.com")
DEFAULT_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme123")

os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(
    title="Leaderboard API",
    description="Multi-tenant fishing tournament leaderboard backend",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

# ============================================================================
# Database Setup
# ============================================================================

def init_database():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            year INTEGER,
            description TEXT,
            total_events INTEGER,
            best_of INTEGER,
            participation_points REAL DEFAULT 50,
            logo_path TEXT,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'admin',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            event_name TEXT NOT NULL,
            event_number INTEGER,
            event_date TEXT,
            status TEXT DEFAULT 'upcoming',
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            category_group TEXT,
            scoring_type TEXT DEFAULT 'points',
            is_standalone INTEGER DEFAULT 0,
            applies_to TEXT,
            unit TEXT DEFAULT 'pts',
            sort_order INTEGER DEFAULT 0,
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            boat_name TEXT,
            captain TEXT,
            owner TEXT,
            angler_name TEXT,
            participant_type TEXT DEFAULT 'boat',
            boat_type TEXT,
            boat_id INTEGER,
            sonar INTEGER DEFAULT 0,
            homeport TEXT,
            photo TEXT,
            website TEXT,
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
            FOREIGN KEY (boat_id) REFERENCES participants(id) ON DELETE SET NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER NOT NULL,
            tournament_id INTEGER NOT NULL,
            participant_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            points REAL NOT NULL,
            notes TEXT,
            UNIQUE(tournament_id, participant_id, category_id),
            FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
            FOREIGN KEY (participant_id) REFERENCES participants(id) ON DELETE CASCADE,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
        )
    """)

    conn.commit()
    conn.close()


@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


# ============================================================================
# Pydantic Models
# ============================================================================

class SeriesUpdate(BaseModel):
    name: Optional[str] = None
    year: Optional[int] = None
    description: Optional[str] = None
    total_events: Optional[int] = None
    best_of: Optional[int] = None
    participation_points: Optional[float] = None
    logo_path: Optional[str] = None
    status: Optional[str] = None

class TournamentCreate(BaseModel):
    event_name: str
    event_number: Optional[int] = None
    event_date: Optional[str] = None
    status: Optional[str] = "upcoming"

class TournamentUpdate(BaseModel):
    event_name: Optional[str] = None
    event_number: Optional[int] = None
    event_date: Optional[str] = None
    status: Optional[str] = None

class CategoryCreate(BaseModel):
    name: str
    category_group: Optional[str] = None
    scoring_type: Optional[str] = "points"
    is_standalone: Optional[bool] = False
    applies_to: Optional[str] = None
    unit: Optional[str] = "pts"
    sort_order: Optional[int] = 0

class ParticipantCreate(BaseModel):
    boat_name: Optional[str] = None
    captain: Optional[str] = None
    owner: Optional[str] = None
    angler_name: Optional[str] = None
    participant_type: Optional[str] = "boat"
    boat_type: Optional[str] = None
    boat_id: Optional[int] = None
    sonar: Optional[bool] = False
    homeport: Optional[str] = None
    photo: Optional[str] = None
    website: Optional[str] = None

class ParticipantUpdate(BaseModel):
    boat_name: Optional[str] = None
    captain: Optional[str] = None
    owner: Optional[str] = None
    angler_name: Optional[str] = None
    participant_type: Optional[str] = None
    boat_type: Optional[str] = None
    boat_id: Optional[int] = None
    sonar: Optional[bool] = None
    homeport: Optional[str] = None
    photo: Optional[str] = None
    website: Optional[str] = None

class PointEntry(BaseModel):
    tournament_id: int
    participant_id: int
    category_id: int
    points: float
    notes: Optional[str] = None

class PointsBatch(BaseModel):
    points: List[PointEntry]

class LoginRequest(BaseModel):
    email: str
    password: str


# ============================================================================
# Auth Helpers
# ============================================================================

def create_token(user_id: int, series_id: int, role: str) -> str:
    expire = datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS)
    return jwt.encode(
        {"sub": str(user_id), "series_id": series_id, "role": role, "exp": expire},
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return decode_token(credentials.credentials)


def require_series_access(slug: str, user: dict = Depends(get_current_user)):
    """Verify user has access to the given series."""
    with get_db() as conn:
        series = conn.execute("SELECT id FROM series WHERE slug = ?", (slug,)).fetchone()
        if not series:
            raise HTTPException(status_code=404, detail="Series not found")
        if user["role"] != "super_admin" and user["series_id"] != series["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
    return user


# ============================================================================
# Helper: resolve series slug to id
# ============================================================================

def get_series_by_slug(slug: str, conn) -> dict:
    row = conn.execute("SELECT * FROM series WHERE slug = ?", (slug,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Series not found")
    return dict(row)


# ============================================================================
# Standings Computation (Python port of JS computeStandings)
# ============================================================================

def compute_standings(
    conn,
    series_id: int,
    category_filter: str = "overall",
    group_filter: str = "all",
    search_term: str = "",
):
    series = dict(conn.execute("SELECT * FROM series WHERE id = ?", (series_id,)).fetchone())
    best_of = series["best_of"] or 3
    part_pts = series["participation_points"] or 0

    tournaments = [
        dict(r) for r in conn.execute(
            "SELECT * FROM tournaments WHERE series_id = ? ORDER BY event_number", (series_id,)
        ).fetchall()
    ]
    tourn_ids = [t["id"] for t in tournaments]

    categories = [
        dict(r) for r in conn.execute(
            "SELECT * FROM categories WHERE series_id = ? ORDER BY sort_order", (series_id,)
        ).fetchall()
    ]

    all_participants = [
        dict(r) for r in conn.execute(
            "SELECT * FROM participants WHERE series_id = ?", (series_id,)
        ).fetchall()
    ]

    all_points = [
        dict(r) for r in conn.execute(
            "SELECT * FROM points WHERE series_id = ?", (series_id,)
        ).fetchall()
    ]

    # Helpers
    boats = [p for p in all_participants if p["participant_type"] == "boat"]
    def get_individuals(ptype):
        return [p for p in all_participants if p["participant_type"] == ptype]
    def get_cat_by_id(cid):
        return next((c for c in categories if c["id"] == cid), None)
    def get_boat_by_id(bid):
        return next((p for p in boats if p["id"] == bid), None)

    is_individual_view = False
    use_participation = False
    unit = "pts"
    selected_cat = None

    if category_filter == "overall":
        candidates = list(boats)
        relevant_cat_ids = [c["id"] for c in categories if not c["is_standalone"]]
        use_participation = True
    else:
        try:
            cat_id = int(category_filter)
        except ValueError:
            cat_id = None
        selected_cat = get_cat_by_id(cat_id) if cat_id else None
        if not selected_cat:
            return {
                "standings": [], "tournaments": tournaments,
                "isIndividualView": False, "unit": "pts",
                "useParticipation": False, "selectedCat": None,
            }

        unit = selected_cat.get("unit", "pts")

        if selected_cat.get("applies_to") in ("sonar", "non_sonar"):
            want_sonar = 1 if selected_cat["applies_to"] == "sonar" else 0
            candidates = [b for b in boats if b["sonar"] == want_sonar]
            relevant_cat_ids = [c["id"] for c in categories if not c["is_standalone"]]
            use_participation = True
        elif selected_cat.get("applies_to"):
            candidates = get_individuals(selected_cat["applies_to"])
            is_individual_view = True
            relevant_cat_ids = [c["id"] for c in categories if not c["is_standalone"]]
            use_participation = True
        else:
            candidates = list(boats)
            relevant_cat_ids = [selected_cat["id"]]
            use_participation = selected_cat["scoring_type"] != "weight"

    # Group filter (boats only)
    if not is_individual_view:
        if group_filter == "private":
            candidates = [p for p in candidates if p.get("boat_type") == "private"]
        elif group_filter == "charter":
            candidates = [p for p in candidates if p.get("boat_type") == "charter"]
        elif group_filter == "sonar":
            candidates = [p for p in candidates if p.get("sonar")]
        elif group_filter == "non_sonar":
            candidates = [p for p in candidates if not p.get("sonar")]

    # Search
    if search_term:
        q = search_term.lower()
        filtered = []
        for p in candidates:
            if q in (p.get("boat_name") or "").lower():
                filtered.append(p); continue
            if q in (p.get("captain") or "").lower():
                filtered.append(p); continue
            if q in (p.get("angler_name") or "").lower():
                filtered.append(p); continue
            if p.get("boat_id"):
                b = get_boat_by_id(p["boat_id"])
                if b and q in (b.get("boat_name") or "").lower():
                    filtered.append(p); continue
        candidates = filtered

    # Build standings
    standings = []
    for part in candidates:
        row = {
            "participant": part,
            "tournamentPoints": {},
            "totalAll": 0,
            "bestOfScore": 0,
            "tournamentsEntered": 0,
            "counted": {},
            "participationBonus": 0,
        }

        for tid in tourn_ids:
            pts = [
                p for p in all_points
                if p["participant_id"] == part["id"]
                and p["tournament_id"] == tid
                and p["category_id"] in relevant_cat_ids
            ]
            total = sum(p["points"] for p in pts)
            row["tournamentPoints"][str(tid)] = total
            if total > 0:
                row["tournamentsEntered"] += 1
            row["totalAll"] += total

        # Best-of-N
        scored = [
            {"tid": tid, "pts": row["tournamentPoints"].get(str(tid), 0)}
            for tid in tourn_ids
        ]
        scored = [s for s in scored if s["pts"] > 0]
        scored.sort(key=lambda s: s["pts"], reverse=True)

        top_n = scored[:best_of]
        best_of_sum = sum(s["pts"] for s in top_n)
        row["participationBonus"] = row["tournamentsEntered"] * part_pts if use_participation else 0
        row["bestOfScore"] = best_of_sum + row["participationBonus"]

        for item in top_n:
            row["counted"][str(item["tid"])] = True

        standings.append(row)

    # Sort
    standings.sort(key=lambda r: (-r["bestOfScore"], -r["totalAll"]))

    # Assign ranks with tie handling
    for i, s in enumerate(standings):
        if i > 0 and s["bestOfScore"] == standings[i-1]["bestOfScore"] and s["totalAll"] == standings[i-1]["totalAll"]:
            s["rank"] = standings[i-1]["rank"]
        else:
            s["rank"] = i + 1

    return {
        "standings": standings,
        "tournaments": tournaments,
        "isIndividualView": is_individual_view,
        "unit": unit,
        "useParticipation": use_participation,
        "selectedCat": selected_cat,
    }


# ============================================================================
# Auth Endpoints
# ============================================================================

@app.post("/api/auth/login")
def login(req: LoginRequest):
    with get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE email = ?", (req.email,)).fetchone()
        if not user or not pwd_context.verify(req.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        token = create_token(user["id"], user["series_id"], user["role"])
        return {"token": token, "email": user["email"], "role": user["role"]}


@app.get("/api/auth/me")
def auth_me(user: dict = Depends(get_current_user)):
    with get_db() as conn:
        row = conn.execute("SELECT id, email, role, series_id FROM users WHERE id = ?", (int(user["sub"]),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        return dict(row)


# ============================================================================
# Public Endpoints
# ============================================================================

@app.get("/api/{slug}")
def get_series(slug: str):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        return s


@app.get("/api/{slug}/standings")
def get_standings(
    slug: str,
    category: str = Query("overall"),
    group: str = Query("all"),
    search: str = Query(""),
):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        return compute_standings(conn, s["id"], category, group, search)


@app.get("/api/{slug}/tournaments")
def list_tournaments(slug: str):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        rows = conn.execute(
            "SELECT * FROM tournaments WHERE series_id = ? ORDER BY event_number", (s["id"],)
        ).fetchall()
        return [dict(r) for r in rows]


@app.get("/api/{slug}/categories")
def list_categories(slug: str):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        rows = conn.execute(
            "SELECT * FROM categories WHERE series_id = ? ORDER BY sort_order", (s["id"],)
        ).fetchall()
        return [dict(r) for r in rows]


@app.post("/api/{slug}/categories")
def create_category(slug: str, data: CategoryCreate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        cur = conn.execute(
            "INSERT INTO categories (series_id, name, category_group, scoring_type, is_standalone, applies_to, unit, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (s["id"], data.name, data.category_group, data.scoring_type,
             1 if data.is_standalone else 0, data.applies_to, data.unit, data.sort_order),
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM categories WHERE id = ?", (cur.lastrowid,)).fetchone())


@app.delete("/api/{slug}/categories/{cid}")
def delete_category(slug: str, cid: int, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        affected = conn.execute(
            "DELETE FROM categories WHERE id = ? AND series_id = ?", (cid, s["id"])
        ).rowcount
        conn.commit()
        if not affected:
            raise HTTPException(status_code=404, detail="Category not found")
        return {"deleted": True}


@app.get("/api/{slug}/participants")
def list_participants(slug: str):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        rows = conn.execute(
            "SELECT * FROM participants WHERE series_id = ?", (s["id"],)
        ).fetchall()
        return [dict(r) for r in rows]


@app.get("/api/{slug}/participants/{participant_id}")
def get_participant(slug: str, participant_id: int):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        row = conn.execute(
            "SELECT * FROM participants WHERE id = ? AND series_id = ?",
            (participant_id, s["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Participant not found")
        return dict(row)


@app.get("/api/{slug}/points")
def list_points(slug: str):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        rows = conn.execute(
            "SELECT * FROM points WHERE series_id = ?", (s["id"],)
        ).fetchall()
        return [dict(r) for r in rows]


# ============================================================================
# Admin Endpoints — Series
# ============================================================================

@app.put("/api/{slug}/series")
def update_series(slug: str, data: SeriesUpdate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        updates = {k: v for k, v in data.model_dump().items() if v is not None}
        if not updates:
            return dict(s)
        updates["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [s["id"]]
        conn.execute(f"UPDATE series SET {set_clause} WHERE id = ?", vals)
        conn.commit()
        return dict(conn.execute("SELECT * FROM series WHERE id = ?", (s["id"],)).fetchone())


# ============================================================================
# Admin Endpoints — Tournaments
# ============================================================================

@app.post("/api/{slug}/tournaments")
def create_tournament(slug: str, data: TournamentCreate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        cur = conn.execute(
            "INSERT INTO tournaments (series_id, event_name, event_number, event_date, status) VALUES (?, ?, ?, ?, ?)",
            (s["id"], data.event_name, data.event_number, data.event_date, data.status),
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM tournaments WHERE id = ?", (cur.lastrowid,)).fetchone())


@app.put("/api/{slug}/tournaments/{tid}")
def update_tournament(slug: str, tid: int, data: TournamentUpdate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        existing = conn.execute(
            "SELECT * FROM tournaments WHERE id = ? AND series_id = ?", (tid, s["id"])
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Tournament not found")
        updates = {k: v for k, v in data.model_dump().items() if v is not None}
        if not updates:
            return dict(existing)
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [tid]
        conn.execute(f"UPDATE tournaments SET {set_clause} WHERE id = ?", vals)
        conn.commit()
        return dict(conn.execute("SELECT * FROM tournaments WHERE id = ?", (tid,)).fetchone())


@app.delete("/api/{slug}/tournaments/{tid}")
def delete_tournament(slug: str, tid: int, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        affected = conn.execute(
            "DELETE FROM tournaments WHERE id = ? AND series_id = ?", (tid, s["id"])
        ).rowcount
        conn.commit()
        if not affected:
            raise HTTPException(status_code=404, detail="Tournament not found")
        return {"deleted": True}


# ============================================================================
# Admin Endpoints — Participants
# ============================================================================

@app.post("/api/{slug}/participants")
def create_participant(slug: str, data: ParticipantCreate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        cur = conn.execute(
            """INSERT INTO participants
               (series_id, boat_name, captain, owner, angler_name, participant_type,
                boat_type, boat_id, sonar, homeport, photo, website)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (s["id"], data.boat_name, data.captain, data.owner, data.angler_name,
             data.participant_type, data.boat_type, data.boat_id,
             1 if data.sonar else 0, data.homeport, data.photo, data.website),
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM participants WHERE id = ?", (cur.lastrowid,)).fetchone())


@app.put("/api/{slug}/participants/{pid}")
def update_participant(slug: str, pid: int, data: ParticipantUpdate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        existing = conn.execute(
            "SELECT * FROM participants WHERE id = ? AND series_id = ?", (pid, s["id"])
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Participant not found")
        updates = {}
        for k, v in data.model_dump().items():
            if v is not None:
                if k == "sonar":
                    updates[k] = 1 if v else 0
                else:
                    updates[k] = v
        if not updates:
            return dict(existing)
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values()) + [pid]
        conn.execute(f"UPDATE participants SET {set_clause} WHERE id = ?", vals)
        conn.commit()
        return dict(conn.execute("SELECT * FROM participants WHERE id = ?", (pid,)).fetchone())


@app.delete("/api/{slug}/participants/{pid}")
def delete_participant(slug: str, pid: int, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        affected = conn.execute(
            "DELETE FROM participants WHERE id = ? AND series_id = ?", (pid, s["id"])
        ).rowcount
        conn.commit()
        if not affected:
            raise HTTPException(status_code=404, detail="Participant not found")
        return {"deleted": True}


# ============================================================================
# Admin Endpoints — Points
# ============================================================================

@app.post("/api/{slug}/points")
def upsert_points(slug: str, data: PointsBatch, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        inserted = 0
        updated = 0
        deleted = 0
        for entry in data.points:
            existing = conn.execute(
                "SELECT id FROM points WHERE tournament_id = ? AND participant_id = ? AND category_id = ?",
                (entry.tournament_id, entry.participant_id, entry.category_id),
            ).fetchone()
            if entry.points <= 0:
                if existing:
                    conn.execute("DELETE FROM points WHERE id = ?", (existing["id"],))
                    deleted += 1
            elif existing:
                conn.execute(
                    "UPDATE points SET points = ?, notes = ? WHERE id = ?",
                    (entry.points, entry.notes, existing["id"]),
                )
                updated += 1
            else:
                conn.execute(
                    "INSERT INTO points (series_id, tournament_id, participant_id, category_id, points, notes) VALUES (?, ?, ?, ?, ?, ?)",
                    (s["id"], entry.tournament_id, entry.participant_id, entry.category_id, entry.points, entry.notes),
                )
                inserted += 1
        conn.commit()
        return {"inserted": inserted, "updated": updated, "deleted": deleted}


@app.delete("/api/{slug}/points/{point_id}")
def delete_point(slug: str, point_id: int, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        affected = conn.execute(
            "DELETE FROM points WHERE id = ? AND series_id = ?", (point_id, s["id"])
        ).rowcount
        conn.commit()
        if not affected:
            raise HTTPException(status_code=404, detail="Point entry not found")
        return {"deleted": True}


# ============================================================================
# Import / Export
# ============================================================================

@app.post("/api/{slug}/import")
async def import_data(slug: str, request: Request, user: dict = Depends(get_current_user)):
    """Import data. If JSON body provided, use it; otherwise read ncbs-data.json from disk."""
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")

        data = None
        body = await request.body()
        if body:
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON body")

        if not data:
            json_path = os.path.join(PARENT_DIR, "ncbs-data.json")
            if not os.path.exists(json_path):
                raise HTTPException(status_code=404, detail="ncbs-data.json not found in parent directory")
            with open(json_path, "r") as f:
                data = json.load(f)

        series_id = s["id"]

        # Update series fields
        sd = data.get("series", {})
        conn.execute(
            "UPDATE series SET name=?, year=?, description=?, total_events=?, best_of=?, participation_points=?, status=?, updated_at=? WHERE id=?",
            (sd.get("name", s["name"]), sd.get("year"), sd.get("description"),
             sd.get("total_events"), sd.get("best_of"), sd.get("participation_points", 50),
             sd.get("status", "active"), datetime.utcnow().isoformat(), series_id),
        )

        # Clear existing data for this series
        conn.execute("DELETE FROM points WHERE series_id = ?", (series_id,))
        conn.execute("DELETE FROM participants WHERE series_id = ?", (series_id,))
        conn.execute("DELETE FROM categories WHERE series_id = ?", (series_id,))
        conn.execute("DELETE FROM tournaments WHERE series_id = ?", (series_id,))

        # ID mapping: old JSON id -> new DB id
        tourn_map = {}
        cat_map = {}
        part_map = {}

        # Tournaments
        for t in data.get("tournaments", []):
            cur = conn.execute(
                "INSERT INTO tournaments (series_id, event_name, event_number, event_date, status) VALUES (?, ?, ?, ?, ?)",
                (series_id, t["event_name"], t.get("event_number"), t.get("event_date"), t.get("status", "upcoming")),
            )
            tourn_map[t["id"]] = cur.lastrowid

        # Categories
        for c in data.get("categories", []):
            cur = conn.execute(
                "INSERT INTO categories (series_id, name, category_group, scoring_type, is_standalone, applies_to, unit, sort_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (series_id, c["name"], c.get("category_group"), c.get("scoring_type", "points"),
                 1 if c.get("is_standalone") else 0, c.get("applies_to"), c.get("unit", "pts"), c.get("sort_order", 0)),
            )
            cat_map[c["id"]] = cur.lastrowid

        # Participants — insert boats first (no boat_id), then individuals
        boats_data = [p for p in data.get("participants", []) if p.get("participant_type", "boat") == "boat"]
        individuals_data = [p for p in data.get("participants", []) if p.get("participant_type", "boat") != "boat"]

        for p in boats_data:
            cur = conn.execute(
                """INSERT INTO participants
                   (series_id, boat_name, captain, owner, angler_name, participant_type,
                    boat_type, boat_id, sonar, homeport, photo, website)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (series_id, p.get("boat_name"), p.get("captain"), p.get("owner"),
                 p.get("angler_name"), "boat", p.get("boat_type"), None,
                 1 if p.get("sonar") else 0, p.get("homeport"), p.get("photo"), p.get("website")),
            )
            part_map[p["id"]] = cur.lastrowid

        for p in individuals_data:
            boat_ref = part_map.get(p.get("boat_id")) if p.get("boat_id") else None
            cur = conn.execute(
                """INSERT INTO participants
                   (series_id, boat_name, captain, owner, angler_name, participant_type,
                    boat_type, boat_id, sonar, homeport, photo, website)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (series_id, p.get("boat_name"), p.get("captain"), p.get("owner"),
                 p.get("angler_name"), p.get("participant_type", "boat"), p.get("boat_type"),
                 boat_ref, 1 if p.get("sonar") else 0, p.get("homeport"), p.get("photo"), p.get("website")),
            )
            part_map[p["id"]] = cur.lastrowid

        # Points
        for pt in data.get("points", []):
            tid = tourn_map.get(pt["tournament_id"])
            pid = part_map.get(pt["participant_id"])
            cid = cat_map.get(pt["category_id"])
            if tid and pid and cid:
                conn.execute(
                    "INSERT INTO points (series_id, tournament_id, participant_id, category_id, points, notes) VALUES (?, ?, ?, ?, ?, ?)",
                    (series_id, tid, pid, cid, pt["points"], pt.get("notes")),
                )

        conn.commit()
        return {
            "imported": True,
            "tournaments": len(tourn_map),
            "categories": len(cat_map),
            "participants": len(part_map),
            "points": len(data.get("points", [])),
        }


@app.post("/api/{slug}/export")
def export_data(slug: str, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")

        sid = s["id"]
        tournaments = [dict(r) for r in conn.execute("SELECT * FROM tournaments WHERE series_id = ? ORDER BY event_number", (sid,)).fetchall()]
        categories = [dict(r) for r in conn.execute("SELECT * FROM categories WHERE series_id = ? ORDER BY sort_order", (sid,)).fetchall()]
        participants = [dict(r) for r in conn.execute("SELECT * FROM participants WHERE series_id = ?", (sid,)).fetchall()]
        points = [dict(r) for r in conn.execute("SELECT * FROM points WHERE series_id = ?", (sid,)).fetchall()]

        return {
            "series": {k: s[k] for k in ("name", "year", "description", "total_events", "best_of", "participation_points", "status")},
            "tournaments": tournaments,
            "categories": categories,
            "participants": participants,
            "points": points,
        }


# ============================================================================
# Photo Upload
# ============================================================================

@app.post("/api/{slug}/upload")
async def upload_photo(slug: str, file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    with get_db() as conn:
        s = get_series_by_slug(slug, conn)
        if user["role"] != "super_admin" and user["series_id"] != s["id"]:
            raise HTTPException(status_code=403, detail="Access denied")

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        raise HTTPException(status_code=400, detail="Only image files allowed")

    safe_name = f"{secrets.token_hex(8)}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)
    content = await file.read()
    with open(path, "wb") as f:
        f.write(content)
    return {"filename": safe_name}


# ============================================================================
# Startup: Init DB, seed default series + admin
# ============================================================================

@app.on_event("startup")
def startup():
    init_database()

    with get_db() as conn:
        # Create default series if none exists
        existing = conn.execute("SELECT id FROM series WHERE slug = 'ncbillfish'").fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO series (slug, name, year, description, total_events, best_of, participation_points, logo_path, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("ncbillfish", "NC Billfish Series", 2026,
                 "North Carolina's premier billfish tournament series",
                 8, 3, 50, "NCBillfishSeries.jpg", "active"),
            )
            conn.commit()

        series = conn.execute("SELECT id FROM series WHERE slug = 'ncbillfish'").fetchone()
        series_id = series["id"]

        # Create default admin if none exists
        existing_user = conn.execute("SELECT id FROM users WHERE email = ?", (DEFAULT_ADMIN_EMAIL,)).fetchone()
        if not existing_user:
            hashed = pwd_context.hash(DEFAULT_ADMIN_PASSWORD)
            conn.execute(
                "INSERT INTO users (series_id, email, password_hash, role) VALUES (?, ?, ?, ?)",
                (series_id, DEFAULT_ADMIN_EMAIL, hashed, "super_admin"),
            )
            conn.commit()
            print(f"Default admin created: {DEFAULT_ADMIN_EMAIL}")


# ============================================================================
# Static File Serving
# ============================================================================

# Serve uploaded photos
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# Serve frontend assets from parent directory (ncbs-leaderboard.html, NCBillfishSeries.jpg)
app.mount("/static", StaticFiles(directory=PARENT_DIR), name="static")


@app.get("/")
def serve_frontend():
    html_path = os.path.join(PARENT_DIR, "ncbs-leaderboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path, media_type="text/html")
    return HTMLResponse("<h1>Leaderboard API</h1><p>Visit <a href='/docs'>/docs</a> for the API.</p>")
